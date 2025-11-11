#!/bin/bash

LOG_DIR=${1:-"logs_parallel"}

if [ ! -d "$LOG_DIR" ]; then
    echo "Error: Log directory '$LOG_DIR' does not exist"
    exit 1
fi

echo "=== Analyzing Logs from $LOG_DIR ==="
echo ""

# Check if processes are still running
if [ -f "$LOG_DIR/pids.txt" ]; then
    PIDS=$(cat "$LOG_DIR/pids.txt")
    TOTAL_PIDS=$(echo $PIDS | wc -w)
    RUNNING=0
    for pid in $PIDS; do
        if ps -p $pid > /dev/null 2>&1; then
            RUNNING=$((RUNNING + 1))
        fi
    done
    echo "Processes still running: $RUNNING / $TOTAL_PIDS"
    echo ""
fi

# Find all log files
LOG_FILES=($(ls -1 "$LOG_DIR"/process_*.log 2>/dev/null | sort -V))

if [ ${#LOG_FILES[@]} -eq 0 ]; then
    echo "No log files found in $LOG_DIR"
    exit 1
fi

echo "Found ${#LOG_FILES[@]} log files"
echo ""

# Summary for each process
echo "=== Per-Process Summary ==="
TOTAL_BLOCKS=0
TOTAL_TIME=0
TOTAL_BPS=0
VALID_PROCESSES=0

for log_file in "${LOG_FILES[@]}"; do
    PROCESS_NUM=$(basename "$log_file" | sed 's/process_\([0-9]*\)\.log/\1/')
    
    if [ ! -f "$log_file" ]; then
        continue
    fi
    
    # Check if process is still running
    STATUS="completed"
    if [ -f "$LOG_DIR/pids.txt" ]; then
        PIDS_ARRAY=($(cat "$LOG_DIR/pids.txt"))
        if [ ${#PIDS_ARRAY[@]} -ge $PROCESS_NUM ]; then
            PID=${PIDS_ARRAY[$((PROCESS_NUM - 1))]}
            if [ ! -z "$PID" ] && ps -p $PID > /dev/null 2>&1; then
                STATUS="running"
            fi
        fi
    fi
    
    echo "Process $PROCESS_NUM ($STATUS):"
    
    # Extract key metrics
    # Note: Log lines have format: [timestamp INFO module] message
    BLOCKS_PROCESSED=$(grep "Blocks actually processed:" "$log_file" | tail -1 | awk '{print $NF}')
    # Total line format: "[timestamp INFO module]   Total:              367939.81ms (100.0%)"
    # Need to extract the number before "ms" - it's the last field before the percentage
    TOTAL_TIME_MS=$(grep "  Total:" "$log_file" | tail -1 | grep -oE '[0-9]+\.[0-9]+ms' | sed 's/ms//')
    # Blocks per second line format: "[timestamp INFO module]   Blocks per second:  0.27"
    BPS=$(grep "Blocks per second:" "$log_file" | tail -1 | awk '{print $NF}')
    
    if [ ! -z "$BLOCKS_PROCESSED" ] && [ "$BLOCKS_PROCESSED" != "0" ]; then
        echo "  Blocks processed: $BLOCKS_PROCESSED"
        if [ ! -z "$TOTAL_TIME_MS" ]; then
            echo "  Total time: ${TOTAL_TIME_MS}ms"
        fi
        if [ ! -z "$BPS" ]; then
            echo "  Blocks per second: $BPS"
            TOTAL_BPS=$(echo "$TOTAL_BPS + $BPS" | bc -l 2>/dev/null || echo "$TOTAL_BPS")
        fi
        TOTAL_BLOCKS=$((TOTAL_BLOCKS + BLOCKS_PROCESSED))
        VALID_PROCESSES=$((VALID_PROCESSES + 1))
    else
        echo "  No blocks processed yet (may still be running)"
    fi
    echo ""
done

# Overall summary
if [ $VALID_PROCESSES -gt 0 ]; then
    echo "=== Overall Summary ==="
    echo "Total blocks processed: $TOTAL_BLOCKS"
    if [ $VALID_PROCESSES -gt 0 ]; then
        AVG_BPS=$(echo "scale=2; $TOTAL_BPS / $VALID_PROCESSES" | bc -l 2>/dev/null || echo "N/A")
        echo "Average blocks per second: $AVG_BPS"
    fi
    echo ""
fi

# Check for errors
echo "=== Error Check ==="
ERROR_COUNT=0
for log_file in "${LOG_FILES[@]}"; do
    PROCESS_NUM=$(basename "$log_file" | sed 's/process_\([0-9]*\)\.log/\1/')
    ERRORS=$(grep -i "error\|failed\|panic" "$log_file" | wc -l)
    if [ $ERRORS -gt 0 ]; then
        echo "Process $PROCESS_NUM: $ERRORS error(s) found"
        ERROR_COUNT=$((ERROR_COUNT + ERRORS))
    fi
done

if [ $ERROR_COUNT -eq 0 ]; then
    echo "No errors found in logs"
else
    echo "Total errors found: $ERROR_COUNT"
fi
echo ""

# Show recent log entries
echo "=== Recent Activity (last 5 lines per process) ==="
for log_file in "${LOG_FILES[@]}"; do
    PROCESS_NUM=$(basename "$log_file" | sed 's/process_\([0-9]*\)\.log/\1/')
    echo "Process $PROCESS_NUM:"
    tail -26 "$log_file" | sed 's/^/  /'
    echo ""
done

