use crate::{
    block::Block,
    calltrace::CallTrace,
    prestate::{DiffTrace, PrestateTrace},
    receipts::BlockReceipts,
};
use alloy::primitives::B256;
use anyhow::{anyhow, Context};
use anyhow::Result;
use rig::log::{debug, warn};
use std::{io::Read, str::FromStr};
use serde_json::json;

/// Converts u64 to hex string with "0x" prefix.
fn to_hex(n: u64) -> String {
    format!("0x{n:x}")
}

/// Fetches the full block data with transactions.
pub fn get_block(endpoint: &str, block_number: u64) -> Result<Block> {
    debug!("RPC: get_block({block_number})");
    let body = json!({
        "method": "eth_getBlockByNumber",
        "params": [to_hex(block_number), true],
        "id": 1,
        "jsonrpc": "2.0"
    });
    let res = send(endpoint, body)?;
    let block = serde_json::from_str(&res)?;
    Ok(block)
}

/// Fetches the block hash.
pub fn get_block_hash(endpoint: &str, block_number: u64) -> Result<B256> {
    debug!("RPC: get_block_hash({block_number})");

    let body = json!({
        "method": "eth_getBlockByNumber",
        "params": [to_hex(block_number), true],
        "id": 1,
        "jsonrpc": "2.0"
    });
    let res = send(endpoint, body)?;
    let res: serde_json::Value = serde_json::from_str(&res)?;
    let hash_hex = res["result"]["hash"]
        .as_str()
        .ok_or_else(|| anyhow!("No block hash found in response"))?;
    let hash = B256::from_str(hash_hex)?;
    Ok(hash)
}

/// Fetches multiple block hashes in batched RPC calls.
/// Chunks requests into batches of 50 to respect rate limits.
/// Returns a HashMap mapping block_number -> B256 hash.
pub fn get_block_hashes_batch(endpoint: &str, block_numbers: &[u64]) -> Result<std::collections::HashMap<u64, B256>> {
    if block_numbers.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    
    const BATCH_SIZE: usize = 40; // Rate limit: 50 requests per second, TODO: adjust this to be more accurate
    let mut all_hashes = std::collections::HashMap::new();
    
    debug!("RPC: get_block_hashes_batch({} blocks) - will be chunked into batches of {}", block_numbers.len(), BATCH_SIZE);
    
    // Process in chunks of BATCH_SIZE to respect rate limits
    let chunks: Vec<_> = block_numbers.chunks(BATCH_SIZE).collect();
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        debug!("RPC: fetching batch {} of {} ({} block hashes)", chunk_idx + 1, chunks.len(), chunk.len());
        
        // Create a batched JSON-RPC request for this chunk
        let batch: Vec<serde_json::Value> = chunk
            .iter()
            .enumerate()
            .map(|(i, &block_num)| {
                json!({
                    "method": "eth_getBlockByNumber",
                    "params": [to_hex(block_num), false], // false = don't need full block, just header
                    "id": i,
                    "jsonrpc": "2.0"
                })
            })
            .collect();
        
        let response = send(endpoint, json!(batch))?;
        
        // Parse the batched response (array of responses)
        let response_value: serde_json::Value = serde_json::from_str(&response)
            .context(format!("Failed to parse batched RPC response for block hashes. Response: {}", response))?;
        
        // Check if it's an array (batched response) or a single object (error)
        let responses = if response_value.is_array() {
            response_value.as_array()
                .ok_or_else(|| anyhow!("Failed to parse response as array"))?
                .clone()
        } else {
            return Err(anyhow!("Expected batched response (array), got single response: {}", response_value));
        };
        
        if responses.len() != chunk.len() {
            return Err(anyhow!("Expected {} responses in batch, got {}. Response: {}", chunk.len(), responses.len(), response));
        }
        
        // Extract results by ID
        for (i, resp) in responses.into_iter().enumerate() {
            // Check if it's a valid response object
            if !resp.is_object() {
                return Err(anyhow!("Expected response object, got: {}", resp));
            }
            
            let id = resp.get("id")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow!("Missing or invalid id in batch response: {}", resp))?;
            
            if id as usize != i {
                return Err(anyhow!("Unexpected id in batch response: expected {}, got {}", i, id));
            }
            
            // Check for errors
            if let Some(error) = resp.get("error") {
                return Err(anyhow!("RPC error in batch response (id={}): {}", id, error));
            }
            
            let result = resp.get("result")
                .ok_or_else(|| anyhow!("Missing result in batch response (id={}). Response object: {}", id, resp))?;
            
            // Extract hash from result
            let hash_hex = result.get("hash")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Missing hash in result for block number {}", chunk[i]))?;
            
            let hash = B256::from_str(hash_hex)?;
            all_hashes.insert(chunk[i], hash);
        }
        
        // Add a delay between batches to respect rate limits (50 requests/second)
        // Only sleep if there are more chunks to process
        if chunk_idx < chunks.len() - 1 {
            // Wait 1.1 seconds before next batch to respect 50 req/s limit
            use std::thread;
            use std::time::Duration;
            thread::sleep(Duration::from_millis(1100)); // TODO Adjust this to be more accurate
        }
    }
    
    Ok(all_hashes)
}

/// Fetches the block receipts.
pub fn get_receipts(endpoint: &str, block_number: u64) -> Result<BlockReceipts> {
    debug!("RPC: get_receipts({block_number})");
    let body = json!({
        "method": "eth_getBlockReceipts",
        "params": [to_hex(block_number)],
        "id": 1,
        "jsonrpc": "2.0"
    });
    let res = send(endpoint, body)?;
    let v = serde_json::from_str(&res)?;
    Ok(v)
}

/// Fetches the prestate trace.
pub fn get_prestate(endpoint: &str, block_number: u64) -> Result<PrestateTrace> {
    debug!("RPC: get_prestate({block_number})");
    let body = json!({
        "method": "debug_traceBlockByNumber",
        "params": [to_hex(block_number), { "tracer": "prestateTracer" }],
        "id": 1,
        "jsonrpc": "2.0"
    });
    let res = send(endpoint, body)?;
    let v = serde_json::from_str(&res)?;
    Ok(v)
}

/// Fetches the diff trace.
pub fn get_difftrace(endpoint: &str, block_number: u64) -> Result<DiffTrace> {
    debug!("RPC: get_difftrace({block_number})");
    let body = json!({
        "method": "debug_traceBlockByNumber",
        "params": [to_hex(block_number), {
            "tracer": "prestateTracer",
            "tracerConfig": { "diffMode": true }
        }],
        "id": 1,
        "jsonrpc": "2.0"
    });
    let res = send(endpoint, body)?;
    let v = serde_json::from_str(&res)?;
    Ok(v)
}

pub fn get_calltrace(endpoint: &str, block_number: u64) -> Result<CallTrace> {
    debug!("RPC: get_calltrace({block_number})");
    use serde::Deserialize;
    use serde_json::Deserializer;

    let body = json!({
        "method": "debug_traceBlockByNumber",
        "params": [to_hex(block_number), {
            "tracer": "callTracer",
        }],
        "id": 1,
        "jsonrpc": "2.0"
    });
    let res = send(endpoint, body)?;

    let mut de = Deserializer::from_str(&res);
    de.disable_recursion_limit();

    let calltrace = CallTrace::deserialize(&mut de)?;
    Ok(calltrace)
}

pub fn get_chain_id(endpoint: &str) -> Result<u64> {
    debug!("RPC: eth_chainId()");
    use serde::Deserialize;
    use serde_json::Deserializer;

    let body = json!({
        "method": "eth_chainId",
        "params": [],
        "id": 1,
        "jsonrpc": "2.0"
    });
    let res = send(endpoint, body)?;
    let res: serde_json::Value = serde_json::from_str(&res)?;
    let s = res["result"].as_str().unwrap();
    let hex = s.trim_start_matches("0x");
    let hex = if hex.is_empty() { "0" } else { hex };
    let id = u64::from_str_radix(hex, 16)?;
    Ok(id)
}

fn send(endpoint: &str, body: serde_json::Value) -> Result<String> {
    use std::time::Instant;
    
    let request_size = serde_json::to_string(&body)?.len();
    let network_start = Instant::now();
    
    let response = ureq::post(endpoint)
        .header("Content-Type", "application/json")
        .header("Accept-Encoding", "zstd, gzip")
        .send_json(body)?;
    
    let network_time = network_start.elapsed();
    
    // Get Content-Encoding header from response
    let content_encoding = response.headers()
        .get("Content-Encoding")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "none".to_string());
    
    // Note: With ureq 3.x and gzip feature enabled, gzip responses are auto-decompressed
    // We need to read the body to get the decompressed content
    // For zstd, we still need to manually decompress
    let read_start = Instant::now();
    let body = response.into_body();
    let mut raw_bytes = Vec::new();
    {
        let mut reader = body.into_reader();
        reader.read_to_end(&mut raw_bytes)?;
    }
    let read_time = read_start.elapsed();
    let raw_size = raw_bytes.len();
    
    debug!("RPC raw response: {} bytes, Content-Encoding: '{}'", 
        raw_size, content_encoding
    );
    
    // Note: With ureq 3.x and gzip feature, gzip responses are automatically decompressed
    // We only need to manually decompress zstd
    let decompressed_bytes = if content_encoding.contains("zstd") {
        let compressed_bytes = raw_bytes;
        let compressed_size = compressed_bytes.len();
        
        // Now decompress (this is the fast CPU part)
        let decompress_start = Instant::now();
        use zstd::stream::Decoder;
        let mut decoder = Decoder::new(&compressed_bytes[..])
            .context("Failed to create zstd decoder")?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .context("Failed to decompress zstd response")?;
        let decompress_time = decompress_start.elapsed();
        
        let space_saved = if decompressed.len() > 0 {
            (1.0 - compressed_size as f64 / decompressed.len() as f64) * 100.0
        } else {
            0.0
        };
        
        debug!("RPC zstd: read={:.2}ms ({} bytes compressed), decompress={:.2}ms ({} bytes decompressed, {:.1}% saved), total={:.2}ms", 
            read_time.as_secs_f64() * 1000.0,
            compressed_size,
            decompress_time.as_secs_f64() * 1000.0,
            decompressed.len(),
            space_saved,
            (read_time + decompress_time).as_secs_f64() * 1000.0
        );
        
        decompressed
    } else {
        // No compression or gzip (ureq auto-decompresses gzip)
        // raw_bytes is already decompressed for gzip, or uncompressed for no compression
        if content_encoding.contains("gzip") {
            let decompressed_size = raw_bytes.len();
            // Content-Length not available (likely using Transfer-Encoding: chunked)
            debug!("RPC gzip: read={:.2}ms ({} bytes decompressed, compressed size unknown (chunked), auto-decompressed by ureq)", 
            read_time.as_secs_f64() * 1000.0,
            decompressed_size
        );
        } else {
            debug!("RPC read: {:.2}ms ({} bytes, uncompressed)", 
                read_time.as_secs_f64() * 1000.0,
                raw_bytes.len()
            );
        }
        raw_bytes
    };
    
    let out = String::from_utf8(decompressed_bytes)
        .context("Response is not valid UTF-8 after decompression")?;
    
    let response_size = out.len();
    
    debug!("RPC network: {:.2}ms (request: {} bytes, response: {} bytes, encoding: {})",
        network_time.as_secs_f64() * 1000.0,
        request_size,
        response_size,
        content_encoding
    );
    
    Ok(out)
}

/// Fetches all block traces in a single batched RPC call.
/// This is much faster than making 5 separate HTTP requests.
/// TODO: maybe we can reduce amount of RPC calls using https://www.quicknode.com/docs/ethereum/qn_getBlockWithReceipts
pub fn get_all_block_traces(
    endpoint: &str,
    block_number: u64,
) -> Result<(Block, PrestateTrace, DiffTrace, BlockReceipts, CallTrace)> {
    debug!("RPC: get_all_block_traces({block_number}) - batched");
    
    let block_hex = to_hex(block_number);
    
    // Create a batched JSON-RPC request with all 5 calls
    let batch = json!([
        {
            "method": "eth_getBlockByNumber",
            "params": [block_hex.clone(), true],
            "id": 0,
            "jsonrpc": "2.0"
        },
        {
            "method": "debug_traceBlockByNumber",
            "params": [block_hex.clone(), { "tracer": "prestateTracer" }],
            "id": 1,
            "jsonrpc": "2.0"
        },
        {
            "method": "debug_traceBlockByNumber",
            "params": [block_hex.clone(), {
                "tracer": "prestateTracer",
                "tracerConfig": { "diffMode": true }
            }],
            "id": 2,
            "jsonrpc": "2.0"
        },
        {
            "method": "eth_getBlockReceipts",
            "params": [block_hex.clone()],
            "id": 3,
            "jsonrpc": "2.0"
        },
        {
            "method": "debug_traceBlockByNumber",
            "params": [block_hex, {
                "tracer": "callTracer",
            }],
            "id": 4,
            "jsonrpc": "2.0"
        }
    ]);
    
    let response = send(endpoint, batch)?;
    
    // Parse the batched response - it should be an array of response objects
    let response_value: serde_json::Value = serde_json::from_str(&response)
        .context(format!("Failed to parse batched RPC response. Response: {}", response))?;
    
    // Check if it's an array (batched response) or a single object (error)
    let responses = if response_value.is_array() {
        response_value.as_array()
            .ok_or_else(|| anyhow!("Failed to parse response as array"))?
            .clone()
    } else {
        // Single response - might be an error
        return Err(anyhow!("Expected batched response (array), got single response: {}", response_value));
    };
    
    if responses.len() != 5 {
        return Err(anyhow!("Expected 5 responses in batch, got {}. Response: {}", responses.len(), response));
    }
    
    // Extract results by ID (they should be in order, but we'll be safe)
    let mut block_result = None;
    let mut prestate_result = None;
    let mut diff_result = None;
    let mut receipts_result = None;
    let mut call_result = None;
    
    for resp in responses {
        // Check if it's a valid response object
        if !resp.is_object() {
            return Err(anyhow!("Expected response object, got: {}", resp));
        }
        
        let id = resp.get("id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow!("Missing or invalid id in batch response: {}", resp))?;
        
        // Check for errors
        if let Some(error) = resp.get("error") {
            return Err(anyhow!("RPC error in batch response (id={}): {}", id, error));
        }
        
        let result = resp.get("result")
            .ok_or_else(|| anyhow!("Missing result in batch response (id={}). Response object: {}", id, resp))?;
        
        match id {
            0 => block_result = Some(result.clone()),
            1 => prestate_result = Some(result.clone()),
            2 => diff_result = Some(result.clone()),
            3 => receipts_result = Some(result.clone()),
            4 => call_result = Some(result.clone()),
            _ => return Err(anyhow!("Unexpected id in batch response: {}", id)),
        }
    }
    
    // Deserialize each result - need to reconstruct full JSON-RPC response structure
    // because Block, PrestateTrace, etc. expect the full response wrapper
    let block_json = json!({
        "jsonrpc": "2.0",
        "result": block_result.ok_or_else(|| anyhow!("Missing block result"))?,
        "id": 0
    });
    let block: Block = serde_json::from_value(block_json)?;
    
    let prestate_json = json!({
        "jsonrpc": "2.0",
        "result": prestate_result.ok_or_else(|| anyhow!("Missing prestate result"))?,
        "id": 1
    });
    let prestate: PrestateTrace = serde_json::from_value(prestate_json)?;
    
    let diff_json = json!({
        "jsonrpc": "2.0",
        "result": diff_result.ok_or_else(|| anyhow!("Missing diff result"))?,
        "id": 2
    });
    let diff: DiffTrace = serde_json::from_value(diff_json)?;
    
    let receipts_json = json!({
        "jsonrpc": "2.0",
        "result": receipts_result.ok_or_else(|| anyhow!("Missing receipts result"))?,
        "id": 3
    });
    let receipts: BlockReceipts = serde_json::from_value(receipts_json)?;
    
    // CallTrace needs special handling due to recursion limit
    use serde::Deserialize;
    use serde_json::Deserializer;
    let call_json = json!({
        "jsonrpc": "2.0",
        "result": call_result.ok_or_else(|| anyhow!("Missing call result"))?,
        "id": 4
    });
    let call_str = serde_json::to_string(&call_json)?;
    let mut de = Deserializer::from_str(&call_str);
    de.disable_recursion_limit();
    let call: CallTrace = CallTrace::deserialize(&mut de)?;
    
    Ok((block, prestate, diff, receipts, call))
}

/// Fetches block traces for multiple blocks in a single batched HTTP request.
/// This is much faster than fetching blocks one at a time.
/// Returns a HashMap mapping block_number -> (Block, PrestateTrace, DiffTrace, BlockReceipts, CallTrace).
/// Only includes successfully fetched blocks in the result.
pub fn get_all_block_traces_batch(
    endpoint: &str,
    block_numbers: &[u64],
) -> Result<std::collections::HashMap<u64, (Block, PrestateTrace, DiffTrace, BlockReceipts, CallTrace)>> {
    if block_numbers.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    
    debug!("RPC: get_all_block_traces_batch({} blocks) - batched", block_numbers.len());
    
    // Create a batched JSON-RPC request with 5 calls per block
    // ID format: (block_index * 5) + call_type
    // call_type: 0=block, 1=prestate, 2=diff, 3=receipts, 4=call
    let mut batch = Vec::new();
    for (block_idx, &block_number) in block_numbers.iter().enumerate() {
        let block_hex = to_hex(block_number);
        let base_id = block_idx * 5;
        
        batch.push(json!({
            "method": "eth_getBlockByNumber",
            "params": [block_hex.clone(), true],
            "id": base_id + 0,
            "jsonrpc": "2.0"
        }));
        
        batch.push(json!({
            "method": "debug_traceBlockByNumber",
            "params": [block_hex.clone(), { "tracer": "prestateTracer" }],
            "id": base_id + 1,
            "jsonrpc": "2.0"
        }));
        
        batch.push(json!({
            "method": "debug_traceBlockByNumber",
            "params": [block_hex.clone(), {
                "tracer": "prestateTracer",
                "tracerConfig": { "diffMode": true }
            }],
            "id": base_id + 2,
            "jsonrpc": "2.0"
        }));
        
        batch.push(json!({
            "method": "eth_getBlockReceipts",
            "params": [block_hex.clone()],
            "id": base_id + 3,
            "jsonrpc": "2.0"
        }));
        
        batch.push(json!({
            "method": "debug_traceBlockByNumber",
            "params": [block_hex, {
                "tracer": "callTracer",
            }],
            "id": base_id + 4,
            "jsonrpc": "2.0"
        }));
    }
    
    let response = send(endpoint, json!(batch))?;
    
    // Parse the batched response
    let parse_start = std::time::Instant::now();
    let response_value: serde_json::Value = serde_json::from_str(&response)
        .context(format!("Failed to parse batched RPC response. Response: {}", response))?;
    let parse_time = parse_start.elapsed();
    
    debug!("RPC parse: {:.2}ms (response size: {} bytes)", 
        parse_time.as_secs_f64() * 1000.0,
        response.len()
    );
    
    let responses = if response_value.is_array() {
        response_value.as_array()
            .ok_or_else(|| anyhow!("Failed to parse response as array"))?
            .clone()
    } else {
        return Err(anyhow!("Expected batched response (array), got single response: {}", response_value));
    };
    
    let expected_responses = block_numbers.len() * 5;
    if responses.len() != expected_responses {
        return Err(anyhow!("Expected {} responses in batch, got {}. Response: {}", expected_responses, responses.len(), response));
    }
    
    // Group responses by block
    let group_start = std::time::Instant::now();
    let mut results = std::collections::HashMap::new();
    
    for block_idx in 0..block_numbers.len() {
        let block_number = block_numbers[block_idx];
        let base_id = block_idx * 5;
        
        // Extract results for this block
        let mut block_result = None;
        let mut prestate_result = None;
        let mut diff_result = None;
        let mut receipts_result = None;
        let mut call_result = None;
        
        for resp in &responses {
            if !resp.is_object() {
                continue;
            }
            
            let id = resp.get("id")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow!("Missing or invalid id in batch response"))?;
            
            // Check if this response belongs to this block
            if id < base_id as u64 || id >= (base_id + 5) as u64 {
                continue;
            }
            
            // Check for errors - skip this block if any call failed
            if let Some(error) = resp.get("error") {
                warn!("RPC error for block {} (id={}): {}", block_number, id, error);
                break;
            }
            
            let result = match resp.get("result") {
                Some(r) => r,
                None => {
                    warn!("Missing result for block {} (id={})", block_number, id);
                    break;
                }
            };
            
            match id as usize - base_id {
                0 => block_result = Some(result.clone()),
                1 => prestate_result = Some(result.clone()),
                2 => diff_result = Some(result.clone()),
                3 => receipts_result = Some(result.clone()),
                4 => call_result = Some(result.clone()),
                _ => {}
            }
        }
        
        // Only add to results if we got all 5 responses
        if let (Some(block_res), Some(prestate_res), Some(diff_res), Some(receipts_res), Some(call_res)) = 
            (block_result, prestate_result, diff_result, receipts_result, call_result) {
            
            // Deserialize each result
            let deserialize_start = std::time::Instant::now();
            let block_json = json!({
                "jsonrpc": "2.0",
                "result": block_res,
                "id": base_id
            });
            let block: Block = serde_json::from_value(block_json)
                .context(format!("Failed to deserialize block for block {}", block_number))?;
            
            let prestate_json = json!({
                "jsonrpc": "2.0",
                "result": prestate_res,
                "id": base_id + 1
            });
            let prestate: PrestateTrace = serde_json::from_value(prestate_json)
                .context(format!("Failed to deserialize prestate for block {}", block_number))?;
            
            let diff_json = json!({
                "jsonrpc": "2.0",
                "result": diff_res,
                "id": base_id + 2
            });
            let diff: DiffTrace = serde_json::from_value(diff_json)
                .context(format!("Failed to deserialize diff for block {}", block_number))?;
            
            let receipts_json = json!({
                "jsonrpc": "2.0",
                "result": receipts_res,
                "id": base_id + 3
            });
            let receipts: BlockReceipts = serde_json::from_value(receipts_json)
                .context(format!("Failed to deserialize receipts for block {}", block_number))?;
            
            // CallTrace needs special handling due to recursion limit
            use serde::Deserialize;
            use serde_json::Deserializer;
            let call_json = json!({
                "jsonrpc": "2.0",
                "result": call_res,
                "id": base_id + 4
            });
            let call_str = serde_json::to_string(&call_json)?;
            let mut de = Deserializer::from_str(&call_str);
            de.disable_recursion_limit();
            let call: CallTrace = CallTrace::deserialize(&mut de)
                .context(format!("Failed to deserialize call trace for block {}", block_number))?;
            
            let deserialize_time = deserialize_start.elapsed();
            if block_idx == 0 {
                debug!("Block {} deserialize: {:.2}ms", block_number, deserialize_time.as_secs_f64() * 1000.0);
            }
            
            results.insert(block_number, (block, prestate, diff, receipts, call));
        } else {
            warn!("Failed to fetch all traces for block {}, skipping", block_number);
        }
    }
    
    let group_time = group_start.elapsed();
    debug!("RPC group/deserialize: {:.2}ms ({} blocks)", 
        group_time.as_secs_f64() * 1000.0,
        block_numbers.len()
    );
    
    Ok(results)
}
