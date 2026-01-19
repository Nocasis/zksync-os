use super::db::{BlockTraces, Database};
use super::rpc;
use anyhow::{Context, Result};
use rig::log::{debug, info, warn};
use std::time::Instant;

const PREFETCH_SIZE: usize = 4; // Prefetch size (4 * 5 = 20 RPC calls, under 50 req/s limit)

/// Fetches block traces from database or RPC endpoint.
///
/// Returns traces from database if available, otherwise fetches from RPC using batched calls.
pub fn fetch_block_traces(block_number: u64, db: &Database, endpoint: &str) -> Result<BlockTraces> {
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

/// Returns a HashMap mapping block_number -> BlockTraces.
/// Blocks already in DB are skipped and returned from cache.
pub fn fetch_block_traces_batch(
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

/// Prefetches the next batch of block traces using batched RPC calls.
///
/// Fetches up to `PREFETCH_SIZE` blocks (default: 4) in a single batched HTTP request and stores
/// them in the cache. This reduces network latency by batching requests and having traces ready
/// when needed. Only prefetches when the cache is empty and skips blocks already in the database.
pub fn prefetch_next_batch(
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
