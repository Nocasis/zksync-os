#!/bin/bash

START_BLOCK=${1:-5187585}
END_BLOCK=${2:-5187704}
DB_PATH=${3:-"eth_runner"}
ENDPOINT=${ENDPOINT:-"https://eth-sepolia.g.alchemy.com/v2/YOUR_KEY"}
WEBHOOK=${WEBHOOK:-""}
NUM_PROCESSES=${NUM_PROCESSES:-6}

TOTAL_BLOCKS=$((END_BLOCK - START_BLOCK + 1))
BLOCKS_PER_PROCESS=$((TOTAL_BLOCKS / NUM_PROCESSES))
REMAINDER=$((TOTAL_BLOCKS % NUM_PROCESSES))

echo "=== Running $NUM_PROCESSES Single-Threaded Processes ==="
echo "Total blocks: $TOTAL_BLOCKS"
echo "Blocks per process: $BLOCKS_PER_PROCESS"
echo "Remainder: $REMAINDER"
echo "Endpoint: $ENDPOINT"
echo ""

# Create log directory
LOG_DIR=${LOG_DIR:-"logs_parallel"}
mkdir -p "$LOG_DIR"

# Start processes
PIDS=()
CURRENT_START=$START_BLOCK

for i in $(seq 1 $NUM_PROCESSES); do
    # Calculate range for this process
    if [ $i -le $REMAINDER ]; then
        # First REMAINDER processes get one extra block
        CURRENT_END=$((CURRENT_START + BLOCKS_PER_PROCESS))
    else
        CURRENT_END=$((CURRENT_START + BLOCKS_PER_PROCESS - 1))
    fi
    
    # Don't exceed end_block
    if [ $CURRENT_END -gt $END_BLOCK ]; then
        CURRENT_END=$END_BLOCK
    fi
    
    if [ $CURRENT_START -le $END_BLOCK ]; then
        echo "Starting process $i: blocks $CURRENT_START to $CURRENT_END"
        
        # Build command with optional webhook
        CMD="RUST_LOG=eth_runner=debug cargo run --manifest-path tests/instances/eth_runner/Cargo.toml --release --features rig/no_print,rig/unlimited_native -- \
            live-run \
            --start-block $CURRENT_START \
            --end-block $CURRENT_END \
            --endpoint \"$ENDPOINT\" \
            --skip-successful \
            --db \"profiling/dbs_parallel/${DB_PATH}_proc${i}\""
        
        # Add webhook if provided
        if [ ! -z "$WEBHOOK" ]; then
            CMD="$CMD --slack-webhook \"$WEBHOOK\""
        fi
        
        nohup bash -c "$CMD" > "$LOG_DIR/process_${i}.log" 2>&1 &
        
        PID=$!
        PIDS+=($PID)
        echo "  Process $i started with PID: $PID"
        CURRENT_START=$((CURRENT_END + 1))
    fi
done

echo ""
echo "Started ${#PIDS[@]} processes. PIDs: ${PIDS[@]}"
echo ""
echo "=== Processes are running in background ==="
echo "You can safely exit SSH. Processes will continue running."
echo ""
echo "To check status, run: ./analyze_logs.sh"
echo "To stop processes, run: pkill -f 'eth_runner.*live-run'"
echo ""
echo "PIDs saved to: $LOG_DIR/pids.txt"
echo "${PIDS[@]}" > "$LOG_DIR/pids.txt"

