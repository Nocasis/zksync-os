use alloy::primitives::U256;
use anyhow::{anyhow, Context, Ok, Result};
mod db;
mod rpc;
use db::{BlockStatus, BlockTraces, Database, ResourceInfo};
use rig::log::{debug, error, info, warn};
use rig::Chain;
use std::time::Instant;
use std::fs;
use std::path::Path;
use zk_ee::system::tracer::NopTracer;

use crate::calltrace::CallTrace;
use crate::native_model::compute_ratio;
use crate::post_check::{post_check, PostCheckError};
use crate::prestate::populate_prestate;
use crate::{
    prestate::{DiffTrace, PrestateTrace},
    receipts::TransactionReceipt,
};
use reqwest::blocking::Client;
use std::backtrace::Backtrace;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
const N_PREV_BLOCKS: usize = 256;
const MAX_FAILURES: usize = 10;
const PREFETCH_SIZE: usize = 4; // Prefetch size (8 * 5 = 40 RPC calls, under 50 req/s limit)

// Global variable to track current block number for panic handler
static CURRENT_BLOCK_NUMBER: AtomicU64 = AtomicU64::new(0);

fn send_slack(webhook: &str, text: &str) -> Result<()> {
    let resp = Client::new()
        .post(webhook)
        .json(&serde_json::json!({ "text": text }))
        .send()?;
    if !resp.status().is_success() {
        return Err(anyhow!("slack webhook returned {}", resp.status()));
    }
    Ok(())
}

fn get_machine_info() -> String {
    let mut info = Vec::new();
    
    // Hostname from environment variable
    if let std::result::Result::Ok(hostname) = std::env::var("HOSTNAME") {
        info.push(format!("Hostname: {}", hostname));
    }
    
    // Process number from environment variable
    if let std::result::Result::Ok(proc_num) = std::env::var("PROC_NUM") {
        info.push(format!("Process Number: {}", proc_num));
    }
    
    // Process info
    info.push(format!("PID: {}", std::process::id()));
    
    info.join("\n")
}

fn install_panic_hook(webhook: Option<String>) {
    panic::set_hook(Box::new(move |info| {
        let current_block = CURRENT_BLOCK_NUMBER.load(Ordering::Relaxed);
        let machine_info = get_machine_info();
        let backtrace = Backtrace::force_capture();
        
        let msg = format!(
            ":rotating_light: eth-runner panicked\n\
            \n\
            *Block Number:* {}\n\
            \n\
            *Machine Info:*\n\
            {}\n\
            \n\
            *Panic Info:*\n\
            {}\n\
            \n\
            *Backtrace:*\n\
            {}",
            if current_block == 0 { "Unknown".to_string() } else { current_block.to_string() },
            machine_info,
            info,
            backtrace
        );
        
        // Always print to stderr
        info!("{msg}");
        
        // Only send to Slack if webhook is provided
        if let Some(webhook_url) = &webhook {
            let _ = send_slack(webhook_url, &msg);
        }
    }));
}

// Fetches hashes for the N_PREV_BLOCKS previous to [start_block].
// Persists them in DB.
// Uses batched RPC call to fetch all missing hashes in a single request.
fn fetch_block_hashes(start_block: u64, db: &Database, endpoint: &str) -> Result<()> {
    let first = start_block.saturating_sub(N_PREV_BLOCKS as u64);
    
    // Collect all block numbers that need to be fetched
    let mut blocks_to_fetch = Vec::new();
    for n in first..start_block {
        if db.get_block_hash(n)?.is_none() {
            blocks_to_fetch.push(n);
        } else {
            debug!("Block hash for {n} already in DB, skipping");
        }
    }
    
    if blocks_to_fetch.is_empty() {
        debug!("All block hashes already in DB, skipping fetch");
        return Ok(());
    }
    
    debug!("Fetching {} block hashes in batched RPC call", blocks_to_fetch.len());
    
    // Fetch all missing hashes in a single batched RPC call
    let hashes = rpc::get_block_hashes_batch(endpoint, &blocks_to_fetch)
        .context(format!("Failed to fetch block hashes in batch"))?;
    
    // Save all hashes to DB
    let blocks_count = blocks_to_fetch.len();
    for block_num in blocks_to_fetch {
        if let Some(hash) = hashes.get(&block_num) {
            db.set_block_hash(block_num, U256::from_be_bytes(hash.0))?;
            debug!("Saved block hash for block {block_num}: {hash:#x}");
        } else {
            return Err(anyhow!("Missing hash for block {block_num} in batched response"));
        }
    }
    
    // Flush all block hash writes after batching
    let flush_start = Instant::now();
    db.flush()?;
    let flush_time = flush_start.elapsed();
    debug!("Flushed {} block hashes in {:.2}ms", blocks_count, flush_time.as_secs_f64() * 1000.0);
    
    Ok(())
}

// Constructs the array of previous N_PREV_BLOCKS block hashes from
// database.
fn get_block_hashes_array(block_number: u64, db: &Database) -> Result<[U256; N_PREV_BLOCKS]> {
    let mut hashes = [U256::ZERO; N_PREV_BLOCKS];
    // Add values for most recent blocks
    for offset in 1..=N_PREV_BLOCKS {
        if let Some(hash) = db.get_block_hash(block_number - (offset as u64))? {
            hashes[N_PREV_BLOCKS - offset] = U256::from(hash);
        } else {
            return Err(anyhow!(format!(
                "DB should have hash for block {}",
                block_number
            )));
        }
    }
    Ok(hashes)
}

// Does not persist the traces.
fn fetch_block_traces(block_number: u64, db: &Database, endpoint: &str) -> Result<BlockTraces> {
    match db.get_block_traces(block_number)? {
        Some(traces) => {
            debug!("Block traces for {block_number} already in DB, skipping");
            Ok(traces)
        }
        None => {
            let rpc_start = Instant::now();
            
            // Use batched RPC call - single HTTP request instead of 5
            let (block, prestate, diff, receipts, call) = rpc::get_all_block_traces(endpoint, block_number)
                .context(format!("Failed to fetch block traces for {block_number}"))?;
            
            let total_rpc_time = rpc_start.elapsed();
            
            debug!("RPC call for block {} (batched): total={:.2}ms",
                block_number,
                total_rpc_time.as_secs_f64() * 1000.0
            );
            
            let block_traces = BlockTraces {
                block,
                prestate,
                diff,
                receipts,
                call,
            };
            Ok(block_traces)
        }
    }
}

/// Fetches block traces for multiple blocks in a single batched HTTP request.
/// Saves block traces to a file for reproduction when a zero address balance error occurs.
fn save_traces_for_reproduction(block_number: u64, traces: &BlockTraces) -> Result<()> {
    let debug_dir = Path::new("debug_reproduction");
    fs::create_dir_all(debug_dir)?;
    
    // Serialize traces to JSON
    let json_data = serde_json::to_string_pretty(traces)
        .context("Failed to serialize traces to JSON")?;
    
    // Compute SHA-256 hash of the JSON data for verification
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(json_data.as_bytes());
    hasher.update(block_number.to_be_bytes());
    let hash = hasher.finalize();
    let hash_hex = hex::encode(hash);
    
    // Save with block number, timestamp, and hash
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let filename = format!("block_{}_zero_addr_error_{}_{}.json", block_number, timestamp, &hash_hex[..8]);
    let filepath = debug_dir.join(&filename);
    
    fs::write(&filepath, &json_data)
        .context(format!("Failed to write traces to {}", filepath.display()))?;
    
    info!("Saved block traces for reproduction: {} (hash: {})", filepath.display(), hash_hex);
    
    // Also save a hash file for quick reference
    let hash_file = debug_dir.join(format!("block_{}_hash.txt", block_number));
    fs::write(&hash_file, format!("Block: {}\nHash: {}\nFile: {}\n", block_number, hash_hex, filename))
        .context(format!("Failed to write hash file to {}", hash_file.display()))?;
    
    Ok(())
}

/// Returns a HashMap mapping block_number -> BlockTraces.
/// Blocks already in DB are skipped and returned from cache.
fn fetch_block_traces_batch(
    block_numbers: &[u64],
    db: &Database,
    endpoint: &str,
) -> Result<std::collections::HashMap<u64, BlockTraces>> {
    let total_start = Instant::now();
    
    let db_check_start = Instant::now();
    let mut blocks_to_fetch = Vec::new();
    let mut results = std::collections::HashMap::new();
    
    for &block_number in block_numbers {
        if let Some(traces) = db.get_block_traces(block_number)? {
            debug!("Block traces for {block_number} already in DB, skipping");
            results.insert(block_number, traces);
        } else {
            blocks_to_fetch.push(block_number);
        }
    }
    let db_check_time = db_check_start.elapsed();
    
    if blocks_to_fetch.is_empty() {
        debug!("All blocks already in DB, skipping RPC fetch");
        return Ok(results);
    }
    
    debug!("Fetching {} blocks in batched RPC call (DB check: {:.2}ms)", 
        blocks_to_fetch.len(),
        db_check_time.as_secs_f64() * 1000.0
    );
    
    let rpc_start = Instant::now();
    let batch_results = rpc::get_all_block_traces_batch(endpoint, &blocks_to_fetch)
        .context("Failed to fetch block traces in batch")?;
    let rpc_time = rpc_start.elapsed();
    
    let parse_start = Instant::now();
    for (block_number, (block, prestate, diff, receipts, call)) in batch_results {
        results.insert(block_number, BlockTraces {
            block,
            prestate,
            diff,
            receipts,
            call,
        });
    }
    let parse_time = parse_start.elapsed();
    
    let total_time = total_start.elapsed();
    info!("Batched RPC call for {} blocks: total={:.2}ms (DB check: {:.2}ms, RPC: {:.2}ms, parse: {:.2}ms, {:.2}ms per block, {:.1} RPC calls)",
        blocks_to_fetch.len(),
        total_time.as_secs_f64() * 1000.0,
        db_check_time.as_secs_f64() * 1000.0,
        rpc_time.as_secs_f64() * 1000.0,
        parse_time.as_secs_f64() * 1000.0,
        total_time.as_secs_f64() * 1000.0 / blocks_to_fetch.len() as f64,
        blocks_to_fetch.len() * 5
    );
    
    Ok(results)
}

#[cfg(feature = "gpu")]
type GpuSharedState = rig::cli_lib::prover_utils::GpuSharedState;

#[cfg(all(feature = "proving", not(feature = "gpu")))]
type GpuSharedState<'a> = rig::cli_lib::prover_utils::GpuSharedState<'a>;

#[cfg(not(feature = "proving"))]
type GpuSharedState = ();

#[allow(clippy::too_many_arguments, unused_variables)]
fn run_block(
    block_number: u64,
    db: &Database,
    endpoint: &str,
    witness_output_dir: Option<String>,
    persist_all: bool,
    chain_id: u64,
    single_tx: Option<u64>,
    gpu_shared_state: &mut Option<&mut GpuSharedState>,
    only_forward: bool,
    profile: Option<String>,
) -> Result<BlockStatus> {
    let block_traces = fetch_block_traces(block_number, db, endpoint)?;
    run_block_with_prefetch(
        block_number,
        db,
        endpoint,
        witness_output_dir,
        persist_all,
        chain_id,
        single_tx,
        gpu_shared_state,
        only_forward,
        profile,
        block_traces,
    )
}

/// Runs a block using prefetched traces.
#[allow(clippy::too_many_arguments, unused_variables)]
fn run_block_with_prefetch(
    block_number: u64,
    db: &Database,
    endpoint: &str,
    witness_output_dir: Option<String>,
    persist_all: bool,
    chain_id: u64,
    single_tx: Option<u64>,
    gpu_shared_state: &mut Option<&mut GpuSharedState>,
    only_forward: bool,
    profile: Option<String>,
    block_traces: BlockTraces,
) -> Result<BlockStatus> {
    let block_start = Instant::now();
    let traces_clone = block_traces.clone();

    let BlockTraces {
        prestate,
        diff,
        block,
        receipts,
        call,
    } = block_traces;
    // set block hash for future blocks to use
    db.set_block_hash(
        block_number,
        U256::from_be_bytes(block.result.header.hash.0),
    )?;
    info!("\n ===================");
    info!("Running block: {block_number}");

    let block_context = block.get_block_context();
    let (transactions, skipped, calls_unsupported_precompile) =
        block.get_transactions(&call, single_tx);
    if calls_unsupported_precompile {
        // Here it makes little sense to run the block, as the post check is gonna fail
        // We just skip it, marking it as successful
        // Hash already flushed above
        warn!("Skipping block {block_number}, as it calls to an unsupported precompile");
        return Ok(BlockStatus::Success);
    }
    info!("Transactions to run: {}", transactions.len());

    let receipts: Vec<TransactionReceipt> = receipts
        .result
        .into_iter()
        .enumerate()
        .filter_map(|(i, x)| if skipped.contains(&i) { None } else { Some(x) })
        .collect();

    let total_gas_used = receipts
        .iter()
        .fold(U256::ZERO, |acc, r| r.gas_used.wrapping_add(acc));
    info!("Reference gas used: {total_gas_used}");

    let ps_trace = PrestateTrace {
        result: prestate
            .result
            .into_iter()
            .enumerate()
            .filter_map(|(i, x)| if skipped.contains(&i) { None } else { Some(x) })
            .collect(),
    };

    let diff_trace = DiffTrace {
        result: diff
            .result
            .into_iter()
            .enumerate()
            .filter_map(|(i, x)| if skipped.contains(&i) { None } else { Some(x) })
            .collect(),
    };

    let calltrace = CallTrace {
        result: call
            .result
            .into_iter()
            .enumerate()
            .filter_map(|(i, x)| if skipped.contains(&i) { None } else { Some(x) })
            .collect(),
    };

    let setup_start = Instant::now();
    let mut chain = Chain::empty_randomized(Some(chain_id));
    chain.set_last_block_number(block_number - 1);

    let db_hash_start = Instant::now();
    chain.set_block_hashes(get_block_hashes_array(block_number, db)?);
    let db_hash_time = db_hash_start.elapsed();

    let prestate_start = Instant::now();
    let prestate_cache = populate_prestate(&mut chain, ps_trace, &calltrace);
    let prestate_time = prestate_start.elapsed();
    let setup_time = setup_start.elapsed();

    let output_path = witness_output_dir.map(|dir| {
        let mut suffix = block_number.to_string();
        suffix.push_str("_witness");
        std::path::Path::new(&dir).join(suffix)
    });
    
    // Set up profiling if requested - include block number in filename
    let profiler_config = profile.map(|profile_path| {
        use std::path::PathBuf;
        let path = if profile_path.ends_with(".svg") {
            // If path ends with .svg, insert block number before extension
            let mut path = PathBuf::from(&profile_path);
            let file_stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("flamegraph");
            let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
            parent.join(format!("{}_block_{}.svg", file_stem, block_number))
        } else {
            // If no extension, append block number
            PathBuf::from(format!("{}_{}", profile_path, block_number))
        };
        let mut profiler = rig::ProfilerConfig::new(path);
        // Sample every 10th cycle for performance
        profiler.frequency_recip = 10;
        profiler
    });
    
    let run_config = rig::chain::RunConfig {
        witness_output_file: output_path,
        only_forward,
        only_forward,
        app: Some("evm_replay".to_string()),
        check_storage_diff_hashes: true,
        profiler_config,
        ..Default::default()
    };
    
    let execution_start = Instant::now();
    
    // Wrap execution in panic handler to catch panics
    let execution_result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        chain.run_block_with_extra_stats(
            transactions,
            Some(block_context),
            None,
            Some(run_config),
            &mut NopTracer::default(),
        )
    }));
    
    let (output, stats, _prover_input) = match execution_result {
        std::result::Result::Ok(std::result::Result::Ok(result)) => result,
        std::result::Result::Ok(std::result::Result::Err(e)) => {
            return Err(anyhow!("Block execution failed: {e:?}"));
        }
        std::result::Result::Err(panic_payload) => {
            // Extract panic message if possible
            let panic_msg = if let Some(s) = panic_payload.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                s.to_string()
            } else {
                format!("Panic occurred (payload type: {:?})", panic_payload.type_id())
            };
            
            error!("Block {block_number} panicked during execution: {panic_msg}");
            return Err(anyhow!("Block {block_number} panicked during execution: {panic_msg}"));
        }
    };
    
    let execution_time = execution_start.elapsed();

    info!("Actual gas used: {}", output.header.gas_used);

    #[cfg(feature = "proving")]
    {
        let bin_path = rig::chain::get_zksync_os_img_path(&Some("evm_replay".to_string()))
            .as_path()
            .to_str()
            .unwrap()
            .to_string();
        let witness: Vec<u8> = _prover_input.iter().flat_map(|x| x.to_be_bytes()).collect();
        let input_hex = hex::encode(witness);
        let non_determinism_data = rig::cli_lib::prover_utils::u32_from_hex_string(&input_hex);
        let binary = rig::cli_lib::prover_utils::load_binary_from_path(&bin_path);
        #[cfg(not(feature = "gpu"))]
        let gpu_shared_state = &mut None;
        let mut total_proof_time = Some(0f64);

        info!("Starting base layer proofs...");
        rig::cli_lib::prover_utils::create_proofs_internal(
            &binary,
            non_determinism_data,
            &rig::cli_lib::Machine::Standard,
            1024,
            None,
            gpu_shared_state,
            &mut total_proof_time,
        );
        info!("Done with base layer proofs");
    }

    let db_write_start = Instant::now();
    if let Some(ratio) = compute_ratio(stats) {
        db.set_block_ratio(block_number, ratio)?;
    }

    let resource_infos: Vec<ResourceInfo> = output
        .tx_results
        .iter()
        .filter_map(|r| {
            r.as_ref().ok().map(|r| ResourceInfo::V0 {
                native_used: r.native_used,
                computational_native_used: r.computational_native_used,
                gas_used: r.gas_used,
                pubdata_used: r.pubdata_used,
                logs_used: r.logs.len() as u64,
            })
        })
        .collect();

    db.set_block_resource_infos(block_number, resource_infos)?;
    
    // Flush once after all writes are batched
    let flush_start = Instant::now();
    db.flush()?;
    let flush_time = flush_start.elapsed();
    let db_write_time = db_write_start.elapsed();

    let post_check_start = Instant::now();
    let post_check_result = post_check(output, receipts, diff_trace, prestate_cache);
    let post_check_time = post_check_start.elapsed();
    
    let total_time = block_start.elapsed();
    
    // Log timing breakdown
    info!("=== Block {} Timing Breakdown ===", block_number);
    // Fetch time is 0 since traces are prefetched
    let fetch_time = std::time::Duration::ZERO;
    info!("  Fetch traces:     {:6.2}ms ({:5.1}%)", fetch_time.as_secs_f64() * 1000.0, fetch_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
    info!("  Setup:             {:6.2}ms ({:5.1}%)", setup_time.as_secs_f64() * 1000.0, setup_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
    info!("    - DB hash read: {:6.2}ms", db_hash_time.as_secs_f64() * 1000.0);
    info!("    - Prestate:     {:6.2}ms", prestate_time.as_secs_f64() * 1000.0);
    info!("  Execution:         {:6.2}ms ({:5.1}%)", execution_time.as_secs_f64() * 1000.0, execution_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
    info!("  Post-check:        {:6.2}ms ({:5.1}%)", post_check_time.as_secs_f64() * 1000.0, post_check_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
    info!("  DB writes:         {:6.2}ms ({:5.1}%)", db_write_time.as_secs_f64() * 1000.0, db_write_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
    info!("    - Flush:         {:6.2}ms ({:5.1}% of DB writes)", flush_time.as_secs_f64() * 1000.0, flush_time.as_secs_f64() / db_write_time.as_secs_f64() * 100.0);
    info!("  Total:             {:6.2}ms", total_time.as_secs_f64() * 1000.0);
    info!("===================================");

    match post_check_result {
        core::result::Result::Ok(()) => {
            let post_db_write_start = Instant::now();
            db.set_block_status(block_number, db::BlockStatus::Success)?;
            if persist_all {
                db.set_block_traces(block_number, &traces_clone)?;
            }
            // Flush status and traces writes
            let post_flush_start = Instant::now();
            db.flush()?;
            let post_flush_time = post_flush_start.elapsed();
            let post_db_write_time = post_db_write_start.elapsed();
            debug!("Post-check DB writes: {:.2}ms (flush: {:.2}ms)", 
                post_db_write_time.as_secs_f64() * 1000.0,
                post_flush_time.as_secs_f64() * 1000.0
            );
            Ok(db::BlockStatus::Success)
        }
        Err(e) => {
            let post_db_write_start = Instant::now();
            db.set_block_status(block_number, db::BlockStatus::Error(e.clone()))?;
            // Always save of them for now, even when already cached.
            // TODO: avoid persisting when read from cache.
            db.set_block_traces(block_number, &traces_clone)?;
            
            // If this is a zero address balance error, save traces to file for reproduction
            // Check for the zero address hex string in the error message
            if let PostCheckError::Internal { msg } = &e {
                if msg.contains("Balance for 0000000000000000000000000000000000000000") {
                    if let Err(save_err) = save_traces_for_reproduction(block_number, &traces_clone) {
                        warn!("Failed to save traces for reproduction: {}", save_err);
                    }
                }
            }
            
            // Flush status and traces writes
            let post_flush_start = Instant::now();
            db.flush()?;
            let post_flush_time = post_flush_start.elapsed();
            let post_db_write_time = post_db_write_start.elapsed();
            debug!("Post-check DB writes: {:.2}ms (flush: {:.2}ms)", 
                post_db_write_time.as_secs_f64() * 1000.0,
                post_flush_time.as_secs_f64() * 1000.0
            );
            debug!("Saved block traces for block {block_number}");
            Ok(db::BlockStatus::Error(e))
        }
    }
}

#[allow(clippy::too_many_arguments, unused_variables)]
fn run_block_with_retries(
    block_number: u64,
    db: &Database,
    endpoint: &str,
    witness_output_dir: Option<String>,
    persist_all: bool,
    chain_id: u64,
    single_tx: Option<u64>,
    gpu_shared_state: &mut Option<&mut GpuSharedState>,
    only_forward: bool,
    backup_endpoint: Option<&String>,
    profile: Option<String>,
) -> Result<BlockStatus> {
    const MAX_RETRIES: usize = 3;

    for attempt in 1..=MAX_RETRIES {
        let endpoint = if attempt == 1 {
            endpoint
        } else if let Some(backup) = backup_endpoint {
            warn!("Switching to backup endpoint for block {block_number} on attempt {attempt}");
            backup.as_str()
        } else {
            endpoint
        };
        match run_block(
            block_number,
            db,
            endpoint,
            witness_output_dir.clone(), // avoid moving on first attempt
            persist_all,
            chain_id,
            single_tx,
            gpu_shared_state,
            only_forward,
            profile.clone(), // Clone to avoid moving on first attempt
        ) {
            core::result::Result::Ok(BlockStatus::Success) => return Ok(BlockStatus::Success),
            e if attempt < MAX_RETRIES => {
                warn!(
                    "Block {block_number} failed on attempt {attempt}/{MAX_RETRIES} with {e:?}, retrying..."
                );
            }
            e => {
                warn!("Block {block_number} failed after {MAX_RETRIES} attempts with {e:?}");
                return e;
            }
        }
    }

    unreachable!()
}

///
/// Run blocks from [start_block] to [end_block].
///
#[allow(clippy::too_many_arguments)]
struct RunStatistics {
    total_block_time: std::time::Duration,
    total_overhead_time: std::time::Duration,
    total_prefetch_time: std::time::Duration,
    blocks_actually_processed: u64,
    blocks_skipped_trace_fetch: u64,
    blocks_skipped_already_succeeded: u64,
    prefetch_hits: u64,
    prefetch_misses: u64,
    total_blocks_prefetched: u64,
    failures: usize,
}

impl RunStatistics {
    fn new() -> Self {
        Self {
            total_block_time: std::time::Duration::ZERO,
            total_overhead_time: std::time::Duration::ZERO,
            total_prefetch_time: std::time::Duration::ZERO,
            blocks_actually_processed: 0,
            blocks_skipped_trace_fetch: 0,
            blocks_skipped_already_succeeded: 0,
            prefetch_hits: 0,
            prefetch_misses: 0,
            total_blocks_prefetched: 0,
            failures: 0,
        }
    }
}

fn prefetch_next_batch(
    next_block_to_prefetch: &mut u64,
    end_block: u64,
    db: &Database,
    endpoint: &str,
    prefetch_cache: &mut std::collections::HashMap<u64, BlockTraces>,
    total_prefetch_time: &mut std::time::Duration,
    total_blocks_prefetched: &mut u64,
) -> Result<()> {
    if prefetch_cache.is_empty() && *next_block_to_prefetch <= end_block {
        let prefetch_timing_start = Instant::now();
        let prefetch_range_end = (*next_block_to_prefetch + PREFETCH_SIZE as u64 - 1).min(end_block);
        
        let prefetch_blocks: Vec<u64> = (*next_block_to_prefetch..=prefetch_range_end)
            .filter(|&block_num| {
                db.get_block_traces(block_num).map(|opt| opt.is_none()).unwrap_or(false)
            })
            .collect();
        
        if !prefetch_blocks.is_empty() {
            debug!("Prefetching {} blocks ({} to {})", 
                prefetch_blocks.len(), 
                prefetch_blocks[0], 
                prefetch_blocks.last().unwrap()
            );
            
            match fetch_block_traces_batch(&prefetch_blocks, db, endpoint) {
                std::result::Result::Ok(batch_results) => {
                    let prefetched_count = batch_results.len() as u64;
                    *total_blocks_prefetched += prefetched_count;
                    prefetch_cache.extend(batch_results);
                    *next_block_to_prefetch = prefetch_range_end + 1;
                    
                    let prefetch_time = prefetch_timing_start.elapsed();
                    *total_prefetch_time += prefetch_time;
                    debug!("Prefetched {} blocks in {:.2}ms ({:.2}ms per block)", 
                        prefetched_count,
                        prefetch_time.as_secs_f64() * 1000.0,
                        prefetch_time.as_secs_f64() * 1000.0 / prefetched_count as f64
                    );
                }
                std::result::Result::Err(e) => {
                    warn!("Failed to prefetch: {}, will fetch individually if needed", e);
                    *next_block_to_prefetch += 1;
                }
            }
        } else {
            *next_block_to_prefetch = prefetch_range_end + 1;
        }
    }
    Ok(())
}

fn try_backup_endpoint(
    block_number: u64,
    primary_result: Result<BlockStatus>,
    backup_endpoint: &str,
    db: &Database,
    witness_output_dir: Option<String>,
    persist_all: bool,
    chain_id: u64,
    single_tx: Option<u64>,
    gpu_state: &mut Option<&mut GpuSharedState>,
    only_forward: bool,
    profile: Option<String>,
    total_block_time: &mut std::time::Duration,
) -> Result<BlockStatus> {
    if let std::result::Result::Ok(BlockStatus::Success) = primary_result {
        return primary_result;
    }
    
    warn!("Block {block_number} failed with primary endpoint. Retrying with backup endpoint...");
    
    let backup_traces_result = {
        let rpc_start = Instant::now();
        match rpc::get_all_block_traces(backup_endpoint, block_number)
            .context(format!("Failed to fetch block traces from backup endpoint for {block_number}"))
        {
            std::result::Result::Ok((block, prestate, diff, receipts, call)) => {
                let total_rpc_time = rpc_start.elapsed();
                debug!("RPC call for block {} from backup endpoint (batched): total={:.2}ms",
                    block_number,
                    total_rpc_time.as_secs_f64() * 1000.0
                );
                std::result::Result::Ok(BlockTraces {
                    block,
                    prestate,
                    diff,
                    receipts,
                    call,
                })
            }
            std::result::Result::Err(e) => std::result::Result::Err(e),
        }
    };
    
    match backup_traces_result {
        std::result::Result::Ok(backup_traces) => {
            let backup_block_start = Instant::now();
            let backup_result = run_block_with_prefetch(
                block_number,
                db,
                backup_endpoint,
                witness_output_dir,
                persist_all,
                chain_id,
                single_tx,
                gpu_state,
                only_forward,
                profile,
                backup_traces,
            );
            let backup_block_time = backup_block_start.elapsed();
            *total_block_time += backup_block_time;
            
            match backup_result {
                std::result::Result::Ok(BlockStatus::Success) => {
                    info!("Block {block_number} succeeded with backup endpoint");
                    std::result::Result::Ok(BlockStatus::Success)
                }
                std::result::Result::Ok(BlockStatus::Error(backup_e)) => {
                    warn!("Block {block_number} also failed with backup endpoint: {backup_e:?}");
                    std::result::Result::Ok(BlockStatus::Error(backup_e))
                }
                std::result::Result::Err(backup_e) => {
                    warn!("Block {block_number} also failed with backup endpoint: {backup_e:?}");
                    std::result::Result::Err(backup_e)
                }
            }
        }
        std::result::Result::Err(fetch_err) => {
            error!("Failed to fetch traces from backup endpoint for block {block_number}: {fetch_err:?}");
            primary_result
        }
    }
}

fn fetch_block_traces_with_backup(
    block_number: u64,
    db: &Database,
    primary_endpoint: &str,
    backup_endpoint: Option<&String>,
    chain_id: u64,
    webhook: Option<&String>,
    stats: &mut RunStatistics,
) -> Result<Option<BlockTraces>> {
    // Try primary endpoint first
    match fetch_block_traces(block_number, db, primary_endpoint) {
        std::result::Result::Ok(traces) => Ok(Some(traces)),
        std::result::Result::Err(primary_err) => {
            error!("Failed to fetch traces for block {block_number} from primary endpoint: {primary_err:?}");
            
            // Try backup endpoint if available
            let traces_result = if let Some(backup) = backup_endpoint {
                warn!("Trying backup endpoint for block {block_number} trace fetch...");
                match rpc::get_all_block_traces(backup, block_number)
                    .context(format!("Failed to fetch block traces from backup endpoint for {block_number}"))
                {
                    std::result::Result::Ok((block, prestate, diff, receipts, call)) => {
                        info!("Successfully fetched traces for block {block_number} from backup endpoint");
                        std::result::Result::Ok(BlockTraces {
                            block,
                            prestate,
                            diff,
                            receipts,
                            call,
                        })
                    }
                    std::result::Result::Err(backup_err) => {
                        error!("Failed to fetch traces for block {block_number} from backup endpoint: {backup_err:?}");
                        std::result::Result::Err(backup_err)
                    }
                }
            } else {
                std::result::Result::Err(primary_err)
            };
            
            match traces_result {
                std::result::Result::Ok(traces) => Ok(Some(traces)),
                std::result::Result::Err(e) => {
                    // Both endpoints failed - send webhook notification and skip block
                    stats.blocks_skipped_trace_fetch += 1;
                    if let Some(webhook) = webhook {
                        let machine_info = get_machine_info();
                        let msg = format!(
                            ":rotating_light: eth_runner: Failed to fetch traces for block {block_number} on chain with id {chain_id}\n\
                            \n\
                            *Block Number:* {block_number}\n\
                            *Chain ID:* {chain_id}\n\
                            \n\
                            *Machine Info:*\n\
                            {machine_info}\n\
                            \n\
                            *Error:*\n\
                            {e:?}"
                        );
                        if let Err(webhook_err) = send_slack(webhook, &msg) {
                            warn!("Failed to send webhook notification: {}", webhook_err);
                        }
                    }
                    
                    // Even if we can't fetch traces, we need to save the block hash
                    // so future blocks can reference it. Try to fetch just the hash.
                    match db.get_block_hash(block_number) {
                        std::result::Result::Ok(Some(_)) => {
                            // Hash already exists, nothing to do
                        }
                        std::result::Result::Ok(None) => {
                            // Hash doesn't exist, try to fetch it
                            match rpc::get_block_hash(primary_endpoint, block_number) {
                                std::result::Result::Ok(hash) => {
                                    if let Err(hash_err) = db.set_block_hash(block_number, U256::from_be_bytes(hash.0)) {
                                        warn!("Failed to save block hash for {block_number}: {hash_err}");
                                    } else {
                                        if let Err(flush_err) = db.flush() {
                                            warn!("Failed to flush DB after saving block hash for {block_number}: {flush_err}");
                                        }
                                        debug!("Saved block hash for block {block_number}");
                                    }
                                }
                                std::result::Result::Err(hash_err) => {
                                    warn!("Failed to fetch block hash for {block_number}: {hash_err}");
                                }
                            }
                        }
                        std::result::Result::Err(_) => {
                            // If get_block_hash returns an error, we just skip saving the hash
                        }
                    }
                    Ok(None) // Return None to indicate block should be skipped
                }
            }
        }
    }
}

fn handle_block_result(
    result: Result<BlockStatus>,
    block_number: u64,
    chain_id: u64,
    webhook: Option<&String>,
    stats: &mut RunStatistics,
) -> Result<()> {
    match result {
        std::result::Result::Ok(BlockStatus::Success) => {
            stats.blocks_actually_processed += 1;
        }
        std::result::Result::Ok(BlockStatus::Error(e)) => {
            stats.failures += 1;
            let webhook_start = Instant::now();
            if let Some(webhook) = webhook {
                let machine_info = get_machine_info();
                let msg = format!(
                    ":rotating_light: eth_runner: Block {block_number} on chain with id {chain_id} failed\n\
                    \n\
                    *Block Number:* {block_number}\n\
                    *Chain ID:* {chain_id}\n\
                    \n\
                    *Machine Info:*\n\
                    {machine_info}\n\
                    \n\
                    *Error:*\n\
                    {e:?}"
                );
                send_slack(webhook, &msg)?;
            }
            stats.total_overhead_time += webhook_start.elapsed();
            
            if stats.failures == MAX_FAILURES {
                error!("Reached max number of failures ({MAX_FAILURES}), stopping execution");
                return Err(anyhow!("Reached max number of failures ({MAX_FAILURES})"));
            }
        }
        std::result::Result::Err(e) => {
            stats.failures += 1;
            error!("Block {block_number} failed with error: {e:?}");
            let webhook_start = Instant::now();
            if let Some(webhook) = webhook {
                let machine_info = get_machine_info();
                let msg = format!(
                    ":rotating_light: eth_runner: Block {block_number} on chain with id {chain_id} failed with execution error\n\
                    \n\
                    *Block Number:* {block_number}\n\
                    *Chain ID:* {chain_id}\n\
                    \n\
                    *Machine Info:*\n\
                    {machine_info}\n\
                    \n\
                    *Error:*\n\
                    {e:?}"
                );
                send_slack(webhook, &msg)?;
            }
            stats.total_overhead_time += webhook_start.elapsed();
            
            if stats.failures == MAX_FAILURES {
                error!("Reached max number of failures ({MAX_FAILURES}), stopping execution");
                return Err(anyhow!("Reached max number of failures ({MAX_FAILURES})"));
            }
        }
    }
    Ok(())
}

fn log_run_statistics(
    start_block: u64,
    end_block: u64,
    chain_id: u64,
    init_time: std::time::Duration,
    total_time: std::time::Duration,
    stats: &RunStatistics,
) {
    let blocks_in_range = (end_block - start_block + 1) as f64;
    let total_overhead_other = total_time
        .saturating_sub(init_time)
        .saturating_sub(stats.total_block_time)
        .saturating_sub(stats.total_overhead_time)
        .saturating_sub(stats.total_prefetch_time);
    
    info!("=== Live Run Completed ===");
    info!("Blocks in range: {} ({} to {})", blocks_in_range as u64, start_block, end_block);
    info!("Blocks actually processed: {}", stats.blocks_actually_processed);
    info!("Blocks skipped (already succeeded): {}", stats.blocks_skipped_already_succeeded);
    info!("Blocks skipped (trace fetch failed): {}", stats.blocks_skipped_trace_fetch);
    info!("Blocks skipped (other): {}", blocks_in_range as u64 - stats.blocks_actually_processed - stats.blocks_skipped_already_succeeded - stats.blocks_skipped_trace_fetch);
    info!("Failures: {}", stats.failures);
    info!("");
    info!("=== Timing Breakdown ===");
    info!("  Initialization:      {:6.2}ms ({:5.1}%)", init_time.as_secs_f64() * 1000.0, init_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
    if stats.blocks_actually_processed > 0 {
        info!("  Block execution:      {:6.2}ms ({:5.1}%)", stats.total_block_time.as_secs_f64() * 1000.0, stats.total_block_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
        info!("  Per-block overhead:  {:6.2}ms ({:5.1}%)", stats.total_overhead_time.as_secs_f64() * 1000.0, stats.total_overhead_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
        info!("    (webhooks, error handling)");
        if stats.total_prefetch_time.as_secs_f64() > 0.0 {
            info!("  Prefetching:          {:6.2}ms ({:5.1}%)", stats.total_prefetch_time.as_secs_f64() * 1000.0, stats.total_prefetch_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
            info!("    Blocks prefetched: {} (avg {:.2}ms per block)", 
                stats.total_blocks_prefetched,
                stats.total_prefetch_time.as_secs_f64() * 1000.0 / stats.total_blocks_prefetched.max(1) as f64
            );
        }
        info!("  Other overhead:      {:6.2}ms ({:5.1}%)", total_overhead_other.as_secs_f64() * 1000.0, total_overhead_other.as_secs_f64() / total_time.as_secs_f64() * 100.0);
        info!("  Total:               {:6.2}ms ({:5.1}%)", total_time.as_secs_f64() * 1000.0, 100.0);
        info!("");
        let avg_time_per_block = stats.total_block_time.as_secs_f64() / stats.blocks_actually_processed as f64;
        let avg_total_per_block = total_time.as_secs_f64() / stats.blocks_actually_processed as f64;
        info!("=== Per Block Averages ===");
        info!("  Execution time:      {:.2}ms", avg_time_per_block * 1000.0);
        info!("  Total time (w/ overhead): {:.2}ms", avg_total_per_block * 1000.0);
        info!("  Blocks per second:  {:.2}", stats.blocks_actually_processed as f64 / total_time.as_secs_f64());
        if stats.prefetch_hits + stats.prefetch_misses > 0 {
            let prefetch_hit_rate = stats.prefetch_hits as f64 / (stats.prefetch_hits + stats.prefetch_misses) as f64 * 100.0;
            info!("=== Prefetch Statistics ===");
            info!("  Prefetch hits:       {} ({:.1}%)", stats.prefetch_hits, prefetch_hit_rate);
            info!("  Prefetch misses:     {} ({:.1}%)", stats.prefetch_misses, 100.0 - prefetch_hit_rate);
            info!("  Total blocks prefetched: {}", stats.total_blocks_prefetched);
        }
    }
    info!("==========================");
}

pub fn live_run(
    start_block: u64,
    end_block: u64,
    endpoint: String,
    db_path: String,
    witness_output_dir: Option<String>,
    skip_successful: bool,
    persist_all: bool,
    webhook: Option<String>,
    single_tx: Option<u64>,
    only_forward: bool,
    backup_endpoint: Option<String>,
    profile: Option<String>,
) -> Result<()> {
    let run_start = Instant::now();
    
    // Install panic hook (with or without webhook)
    install_panic_hook(webhook.clone());
    
    let init_start = Instant::now();
    let db = Database::init(db_path)?;
    assert!(start_block <= end_block);
    fetch_block_hashes(start_block, &db, &endpoint)?;
    let chain_id = rpc::get_chain_id(&endpoint)?;
    let init_time = init_start.elapsed();
    let mut failures = 0;
    
    info!("=== Live Run Started ===");
    info!("Blocks: {} to {}", start_block, end_block);
    info!("Initialization: {:.2}ms", init_time.as_secs_f64() * 1000.0);

    #[cfg(feature = "gpu")]
    let mut gpu_state = {
        info!("Setting up GPU state...");
        let bin_path = rig::chain::get_zksync_os_img_path(&Some("evm_replay".to_string()))
            .as_path()
            .to_str()
            .unwrap()
            .to_string();
        let binary = rig::cli_lib::prover_utils::load_binary_from_path(&bin_path);
        let s = rig::cli_lib::prover_utils::GpuSharedState::new(
            &binary,
            rig::gpu_prover::circuit_type::MainCircuitType::ReducedRiscVMachine,
        );
        info!("Done setting up GPU state...");
        s
    };
    #[cfg(feature = "gpu")]
    let gpu_state = &mut Some(&mut gpu_state);

    #[cfg(not(feature = "gpu"))]
    let gpu_state: &mut Option<&mut GpuSharedState> = &mut None;

    let mut stats = RunStatistics::new();
    let mut prefetch_cache = std::collections::HashMap::<u64, BlockTraces>::new();
    let mut next_block_to_prefetch = start_block;
    let mut stopped_early = false;
    
    for n in start_block..=end_block {
        // Update current block number for panic handler
        CURRENT_BLOCK_NUMBER.store(n, Ordering::Relaxed);
        
        // Prefetch next batch if cache is empty
        prefetch_next_batch(
            &mut next_block_to_prefetch,
            end_block,
            &db,
            &endpoint,
            &mut prefetch_cache,
            &mut stats.total_prefetch_time,
            &mut stats.total_blocks_prefetched,
        )?;
        
        // Check if we should skip this block
        if let std::result::Result::Ok(Some(status)) = db.get_block_status(n) {
            if skip_successful && matches!(status, BlockStatus::Success) {
                debug!("Skipping block {n}, already succeeded");
                stats.blocks_skipped_already_succeeded += 1;
                continue;
            }
        }
        
        // Get traces from prefetch cache or fetch if not available
        let block_traces = if let Some(traces) = prefetch_cache.remove(&n) {
            stats.prefetch_hits += 1;
            traces
        } else {
            stats.prefetch_misses += 1;
            match fetch_block_traces_with_backup(
                n,
                &db,
                &endpoint,
                backup_endpoint.as_ref(),
                chain_id,
                webhook.as_ref(),
                &mut stats,
            )? {
                Some(traces) => traces,
                None => continue, // Block skipped due to trace fetch failure
            }
        };
        
        // Process block sequentially
        let block_start = Instant::now();
        let primary_result = run_block_with_prefetch(
            n,
            &db,
            &endpoint,
            witness_output_dir.clone(),
            persist_all,
            chain_id,
            single_tx,
            gpu_state,
            only_forward,
            profile.clone(),
            block_traces,
        );
        let block_time = block_start.elapsed();
        stats.total_block_time += block_time;
        
        // Try backup endpoint if primary execution failed
        let result = if let Some(backup) = backup_endpoint.as_ref() {
            try_backup_endpoint(
                n,
                primary_result,
                backup,
                &db,
                witness_output_dir.clone(),
                persist_all,
                chain_id,
                single_tx,
                gpu_state,
                only_forward,
                profile.clone(),
                &mut stats.total_block_time,
            )
        } else {
            primary_result
        };
        
        // Handle result (update stats, send webhooks, check failures)
        // If max failures reached, break out of loop gracefully
        if let Err(e) = handle_block_result(result, n, chain_id, webhook.as_ref(), &mut stats) {
            warn!("Stopping execution: {e}");
            stopped_early = true;
            break;
        }
    }
    
    let total_time = run_start.elapsed();
    log_run_statistics(start_block, end_block, chain_id, init_time, total_time, &stats);
    
    if let Some(webhook) = webhook.as_ref() {
        let machine_info = get_machine_info();
        let (emoji, status_msg) = if stopped_early {
            (":rotating_light:", "stopped early due to max failures")
        } else {
            (":white_check_mark:", "successfully!")
        };
        let msg = format!(
            "{emoji} eth_runner: finished running from block {start_block} to {end_block} on chain with id {chain_id} {status_msg}\n\
            \n\
            *Block Range:* {start_block} to {end_block}\n\
            *Chain ID:* {chain_id}\n\
            *Blocks Processed:* {}\n\
            *Blocks Skipped (already succeeded):* {}\n\
            *Blocks Skipped (trace fetch failed):* {}\n\
            *Failures:* {}\n\
            \n\
            *Machine Info:*\n\
            {machine_info}",
            stats.blocks_actually_processed,
            stats.blocks_skipped_already_succeeded,
            stats.blocks_skipped_trace_fetch,
            stats.failures
        );
        send_slack(webhook, &msg)?
    }
    if let Some(webhook) = webhook.as_ref() {
        let msg = format!(":white_check_mark: eth_runner: finished running from block {start_block} to {end_block} on chain with id {chain_id} successfully!");
        send_slack(webhook, &msg)?
    }
    Ok(())
}

///
/// Export native/effective cycles ratios to csv file.
///
pub fn export_block_ratios(db: String, path: Option<String>) -> Result<()> {
    let db = Database::init(db)?;
    let path = path.unwrap_or("ratios.csv".to_string());
    db.export_block_ratios_to_csv(&path)?;
    db.export_block_resource_info_to_csv("resource_info.csv")?;
    Ok(())
}

///
/// Show failed blocks, if any.
///
pub fn show_status(db: String) -> Result<()> {
    let db = Database::init(db)?;
    let failures = db.iter_failed_block_statuses()?;
    if failures.is_empty() {
        println!("✅ All blocks succeeded.");
        Ok(())
    } else {
        println!("❌ Failed blocks:");
        for (block_number, status) in failures {
            println!("Block {block_number:<8} => {status:?}");
        }
        Ok(())
    }
}
