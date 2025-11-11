use alloy::primitives::U256;
use anyhow::{anyhow, Context, Ok, Result};
mod db;
mod rpc;
use db::{BlockStatus, BlockTraces, Database, ResourceInfo};
use rig::log::{debug, error, info, warn};
use rig::Chain;
use std::time::Instant;
use zk_ee::system::tracer::NopTracer;

use crate::calltrace::CallTrace;
use crate::native_model::compute_ratio;
use crate::post_check::post_check;
use crate::prestate::populate_prestate;
use crate::{
    prestate::{DiffTrace, PrestateTrace},
    receipts::TransactionReceipt,
};
use reqwest::blocking::Client;
use serde_json::json;
use std::backtrace::Backtrace;
use std::panic;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

const N_PREV_BLOCKS: usize = 256;
const MAX_FAILURES: usize = 10;
const PREFETCH_SIZE: usize = 8; // Prefetch 8 blocks ahead (8 * 5 = 40 RPC calls, under 50 req/s limit) // TODO: adjust this value
const PARALLEL_THREADS: usize = 8; // Number of threads for parallel block execution

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

fn install_panic_hook(webhook: String) {
    panic::set_hook(Box::new(move |info| {
        let msg = format!(
            ":rotating_light: eth-runner panicked: {info}\n{}",
            Backtrace::force_capture()
        );
        let _ = Client::new()
            .post(&webhook)
            .json(&json!({ "text": msg }))
            .send();
        eprintln!("{msg}");
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
    // IMPORTANT: Flush immediately so parallel blocks can see this hash
    db.set_block_hash(
        block_number,
        U256::from_be_bytes(block.result.header.hash.0),
    )?;
    db.flush()?; // Flush hash immediately for parallel execution
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
        app: Some("evm_replay".to_string()),
        check_storage_diff_hashes: true,
        profiler_config,
        ..Default::default()
    };
    
    let execution_start = Instant::now();
    let (output, stats, _prover_input) = chain
        .run_block_with_extra_stats(
            transactions,
            Some(block_context),
            Some(run_config),
            &mut NopTracer::default(),
        )
        .unwrap();
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
    profile: Option<String>,
) -> Result<BlockStatus> {
    const MAX_RETRIES: usize = 3;

    for attempt in 1..=MAX_RETRIES {
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
    profile: Option<String>,
) -> Result<()> {
    let run_start = Instant::now();
    
    if let Some(webhook) = webhook.clone() {
        install_panic_hook(webhook);
    }
    
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

    let mut total_block_time = std::time::Duration::ZERO; // Sum of all block execution times (for averages)
    let mut total_parallel_execution_time = std::time::Duration::ZERO; // Actual wall-clock time for parallel execution
    let mut total_overhead_time = std::time::Duration::ZERO;
    let mut total_prefetch_time = std::time::Duration::ZERO;
    let mut blocks_actually_processed = 0u64;
    let mut prefetch_hits = 0u64;
    let mut prefetch_misses = 0u64;
    let mut total_blocks_prefetched = 0u64;
    
    let mut prefetch_cache = std::collections::HashMap::<u64, BlockTraces>::new();
    let mut next_block_to_prefetch = start_block;
    let mut current_block = start_block;
    
    // Create a thread pool for parallel block execution
    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(PARALLEL_THREADS)
        .build()
        .context("Failed to create thread pool")?;
    
    while current_block <= end_block {
        // Prefetch next batch if cache is empty and we haven't reached end_block
        // This implements batch-based prefetching: prefetch PREFETCH_SIZE blocks, execute them in parallel (PARALLEL_THREADS threads), then prefetch next batch
        if prefetch_cache.is_empty() && next_block_to_prefetch <= end_block {
            let prefetch_timing_start = Instant::now();
            let prefetch_range_end = (next_block_to_prefetch + PREFETCH_SIZE as u64 - 1).min(end_block);
            
            if next_block_to_prefetch <= prefetch_range_end {
                let prefetch_blocks: Vec<u64> = (next_block_to_prefetch..=prefetch_range_end)
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
                    
                    match fetch_block_traces_batch(&prefetch_blocks, &db, &endpoint) {
                        std::result::Result::Ok(batch_results) => {
                            let prefetched_count = batch_results.len() as u64;
                            total_blocks_prefetched += prefetched_count;
                            prefetch_cache.extend(batch_results);
                            next_block_to_prefetch = prefetch_blocks.last().copied().unwrap_or(next_block_to_prefetch) + 1;
                            
                            let prefetch_time = prefetch_timing_start.elapsed();
                            total_prefetch_time += prefetch_time;
                            debug!("Prefetched {} blocks in {:.2}ms ({:.2}ms per block)", 
                                prefetched_count,
                                prefetch_time.as_secs_f64() * 1000.0,
                                prefetch_time.as_secs_f64() * 1000.0 / prefetched_count as f64
                            );
                        }
                        std::result::Result::Err(_) => {
                            warn!("Failed to prefetch blocks, will fetch individually if needed");
                            // On error, skip to next block to avoid infinite loop
                            next_block_to_prefetch += 1;
                        }
                    }
                } else {
                    // All blocks in range are already in DB, skip to next batch
                    next_block_to_prefetch = prefetch_range_end + 1;
                }
            }
        }
        
        // Collect blocks to process in this batch (from prefetch_cache, in order)
        let mut blocks_to_process: Vec<u64> = prefetch_cache.keys()
            .copied()
            .filter(|&n| n >= current_block && n <= end_block)
            .collect();
        blocks_to_process.sort();
        
        // Limit to PREFETCH_SIZE to process one batch at a time
        if blocks_to_process.len() > PREFETCH_SIZE {
            blocks_to_process.truncate(PREFETCH_SIZE);
        }
        
        if blocks_to_process.is_empty() {
            // No blocks to process, advance to next block
            current_block += 1;
            continue;
        }
        
        // Process blocks in parallel
        let batch_start = Instant::now();
        let db_clone = db.clone();
        let endpoint_clone = endpoint.clone();
        let witness_output_dir_clone = witness_output_dir.clone();
        let profile_clone = profile.clone();
        
        // Prepare data for parallel processing
        let mut blocks_with_traces: Vec<(u64, BlockTraces)> = Vec::new();
        let mut blocks_to_fetch: Vec<u64> = Vec::new();
        
        for &n in &blocks_to_process {
            // Check if we should skip this block
            if let Result::Ok(Some(status)) = db_clone.get_block_status(n) {
                if skip_successful {
                    if let BlockStatus::Success = status {
                        debug!("Skipping block {n}, already succeeded");
                        continue;
                    }
                }
            }
            
            if let Some(traces) = prefetch_cache.remove(&n) {
                blocks_with_traces.push((n, traces));
                prefetch_hits += 1;
            } else {
                blocks_to_fetch.push(n);
                prefetch_misses += 1;
            }
        }
        
        // Fetch any blocks that weren't in the prefetch cache
        for n in blocks_to_fetch {
            match fetch_block_traces(n, &db_clone, &endpoint_clone) {
                Result::Ok(traces) => blocks_with_traces.push((n, traces)),
                Result::Err(e) => {
                    error!("Failed to fetch traces for block {n}: {e:?}");
                    // Continue with other blocks
                }
            }
        }
        
        if blocks_with_traces.is_empty() {
            current_block = blocks_to_process.last().copied().unwrap_or(current_block) + 1;
            continue;
        }
        
        info!("Processing {} blocks in parallel ({} to {})", 
            blocks_with_traces.len(),
            blocks_with_traces[0].0,
            blocks_with_traces.last().unwrap().0
        );
        
        // Process blocks in parallel using rayon (limited to PARALLEL_THREADS threads)
        // IMPORTANT: Blocks are processed in order, so each block can safely read the previous block's hash
        // after it's been written and flushed by the previous block
        let results: Vec<(u64, Result<BlockStatus>, std::time::Duration)> = thread_pool.install(|| {
            blocks_with_traces
                .into_par_iter()
                .map(|(n, block_traces)| {
                let block_start = Instant::now();
                let db_local = db_clone.clone();
                
                // Wait for previous block's hash to be available (if previous block exists)
                // This ensures we don't race when reading hashes in get_block_hashes_array
                if n > start_block {
                    let prev_block = n - 1;
                    let mut retries = 0;
                    const MAX_HASH_WAIT_RETRIES: usize = 100;
                    loop {
                        match db_local.get_block_hash(prev_block) {
                            Result::Ok(Some(_)) => break, // Hash is available
                            Result::Ok(None) if retries < MAX_HASH_WAIT_RETRIES => {
                                std::thread::sleep(std::time::Duration::from_millis(10));
                                retries += 1;
                            }
                            Result::Ok(None) => {
                                return (n, Err(anyhow!("Previous block {} hash not available after waiting", prev_block)), block_start.elapsed());
                            }
                            Result::Err(e) => {
                                return (n, Err(anyhow!("Failed to get hash for previous block {}: {}", prev_block, e)), block_start.elapsed());
                            }
                        }
                    }
                }
                
                // Note: GPU state cannot be shared across threads, so we pass None for parallel execution
                // GPU proving will be disabled for parallel execution
                let result = {
                    let mut gpu_state_none: Option<&mut GpuSharedState> = None;
                    run_block_with_prefetch(
                        n,
                        &db_local,
                        &endpoint_clone,
                        witness_output_dir_clone.clone(),
                        persist_all,
                        chain_id,
                        single_tx,
                        &mut gpu_state_none as &mut Option<&mut GpuSharedState>,
                        only_forward,
                        profile_clone.clone(),
                        block_traces,
                    )
                };
                let block_time = block_start.elapsed();
                (n, result, block_time)
            })
            .collect()
        });
        
        let batch_time = batch_start.elapsed();
        total_parallel_execution_time += batch_time;
        
        // Process results sequentially for error handling and stats
        for (n, block_result, block_time) in results {
            match block_result {
                Result::Ok(BlockStatus::Success) => {
                    blocks_actually_processed += 1;
                    total_block_time += block_time;
                }
                Result::Ok(BlockStatus::Error(e)) => {
                    failures += 1;
                    let webhook_start = Instant::now();
                    if let Some(webhook) = webhook.as_ref() {
                        let msg = format!(":rotating_light: eth_runner: Block {n} on chain with id {chain_id} failed with: {e:?}");
                        send_slack(webhook, &msg)?;
                    }
                    total_overhead_time += webhook_start.elapsed();
                    
                    if failures == MAX_FAILURES {
                        error!("Reached max number of failures");
                        panic!()
                    }
                }
                Result::Err(e) => {
                    failures += 1;
                    error!("Block {n} failed with error: {e:?}");
                    if failures == MAX_FAILURES {
                        error!("Reached max number of failures");
                        panic!()
                    }
                }
            }
        }
        
        // Advance to next batch
        current_block = blocks_to_process.last().copied().unwrap_or(current_block) + 1;
    }
    
    let total_time = run_start.elapsed();
    let blocks_in_range = (end_block - start_block + 1) as f64;
    let total_overhead_other = total_time
        .saturating_sub(init_time)
        .saturating_sub(total_parallel_execution_time)
        .saturating_sub(total_overhead_time)
        .saturating_sub(total_prefetch_time);
    
    info!("=== Live Run Completed ===");
    info!("Blocks in range: {} ({} to {})", blocks_in_range as u64, start_block, end_block);
    info!("Blocks actually processed: {}", blocks_actually_processed);
    info!("Blocks skipped: {}", blocks_in_range as u64 - blocks_actually_processed);
    info!("Failures: {}", failures);
    info!("");
    info!("=== Timing Breakdown ===");
    info!("  Initialization:      {:6.2}ms ({:5.1}%)", init_time.as_secs_f64() * 1000.0, init_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
    if blocks_actually_processed > 0 {
        info!("  Block execution:      {:6.2}ms ({:5.1}%)", total_parallel_execution_time.as_secs_f64() * 1000.0, total_parallel_execution_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
        info!("  Per-block overhead:  {:6.2}ms ({:5.1}%)", total_overhead_time.as_secs_f64() * 1000.0, total_overhead_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
        info!("    (status checks, loop, webhooks)");
        if total_prefetch_time.as_secs_f64() > 0.0 {
            info!("  Prefetching:          {:6.2}ms ({:5.1}%)", total_prefetch_time.as_secs_f64() * 1000.0, total_prefetch_time.as_secs_f64() / total_time.as_secs_f64() * 100.0);
            info!("    Blocks prefetched: {} (avg {:.2}ms per block)", 
                total_blocks_prefetched,
                total_prefetch_time.as_secs_f64() * 1000.0 / total_blocks_prefetched.max(1) as f64
            );
        }
    }
    info!("  Other overhead:      {:6.2}ms ({:5.1}%)", total_overhead_other.as_secs_f64() * 1000.0, total_overhead_other.as_secs_f64() / total_time.as_secs_f64() * 100.0);
    info!("  Total:               {:6.2}ms ({:5.1}%)", total_time.as_secs_f64() * 1000.0, 100.0);
    info!("");
    if blocks_actually_processed > 0 {
        let avg_time_per_block = total_block_time.as_secs_f64() / blocks_actually_processed as f64;
        let avg_total_per_block = total_time.as_secs_f64() / blocks_actually_processed as f64;
        let avg_parallel_time_per_block = total_parallel_execution_time.as_secs_f64() / blocks_actually_processed as f64;
        info!("=== Per Block Averages ===");
        info!("  Execution time:      {:.2}ms", avg_time_per_block * 1000.0);
        info!("  Total time (w/ overhead): {:.2}ms", avg_total_per_block * 1000.0);
        info!("  Parallel execution time: {:.2}ms", avg_parallel_time_per_block * 1000.0);
        info!("  Blocks per second (total):   {:.2}", blocks_actually_processed as f64 / total_time.as_secs_f64());
        if total_parallel_execution_time.as_secs_f64() > 0.0 {
            info!("  Blocks per second (parallel execution): {:.2}", blocks_actually_processed as f64 / total_parallel_execution_time.as_secs_f64());
        }
        if prefetch_hits + prefetch_misses > 0 {
            let prefetch_hit_rate = prefetch_hits as f64 / (prefetch_hits + prefetch_misses) as f64 * 100.0;
            info!("=== Prefetch Statistics ===");
            info!("  Prefetch hits:       {} ({:.1}%)", prefetch_hits, prefetch_hit_rate);
            info!("  Prefetch misses:     {} ({:.1}%)", prefetch_misses, 100.0 - prefetch_hit_rate);
            info!("  Total blocks prefetched: {}", total_blocks_prefetched);
        }
    }
    info!("==========================");
    
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
