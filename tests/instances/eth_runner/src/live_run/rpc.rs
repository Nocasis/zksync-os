use crate::{
    block::Block,
    calltrace::CallTrace,
    prestate::{DiffTrace, PrestateTrace},
    receipts::BlockReceipts,
};
use alloy::primitives::B256;
use anyhow::{anyhow, Context};
use anyhow::Result;
use rig::log::debug;
use std::{io::Read, str::FromStr};
use ureq::json;

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
    let response = ureq::post(endpoint)
        .set("Content-Type", "application/json")
        .send_json(body)?;

    let mut out = String::new();
    response.into_reader().read_to_string(&mut out)?;
    Ok(out)
}

/// Fetches all block traces in a single batched RPC call.
/// This is much faster than making 5 separate HTTP requests.
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
