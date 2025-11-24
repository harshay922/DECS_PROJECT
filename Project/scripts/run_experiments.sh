#!/bin/bash

# Automated experiment runner for CS744 Project
# Usage: ./run_experiments.sh <server_url> <output_dir>

SERVER_URL=$1
OUTPUT_DIR=$2
THREADS=(10 25 50 75 100)  # Different load levels
DURATION=300               # 5 minutes per test (as required by spec)
KEYSPACE=10000
POPULAR_KEYS=100

mkdir -p "$OUTPUT_DIR"

# Workload configurations
declare -A WORKLOADS
WORKLOADS=(
    ["put_all"]="put_all"
    ["get_all"]="get_all" 
    ["get_popular"]="get_popular"
    ["mixed"]="mixed 0.4 0.4 0.2"
    ["cpu_bound"]="cpu_bound"
)

echo "Starting CS744 Performance Experiments"
echo "Server: $SERVER_URL"
echo "Output Directory: $OUTPUT_DIR"
echo "========================================"

for workload in "${!WORKLOADS[@]}"; do
    echo "\n=== Running $workload workload ==="
    
    for threads in "${THREADS[@]}"; do
        echo "Testing with $threads threads..."
        
        # Start resource monitoring
        ./monitor_resources.sh $DURATION "$OUTPUT_DIR/${workload}_${threads}th" &
        MONITOR_PID=$!
        
        # Run load test
        OUTPUT_FILE="$OUTPUT_DIR/${workload}_${threads}th.txt"
        ./loadgen "$SERVER_URL" $threads $DURATION ${WORKLOADS[$workload]} $KEYSPACE $POPULAR_KEYS > "$OUTPUT_FILE" 2>&1
        
        # Wait for monitoring to complete
        wait $MONITOR_PID
        
        # Get server stats
        curl -s "$SERVER_URL/stats" > "$OUTPUT_DIR/${workload}_${threads}th_stats.json"
        
        echo "Completed $threads threads for $workload"
        sleep 10  # Cool-down period between tests
    done
    
    echo "=== Completed $workload workload ==="
    sleep 30  # Longer cool-down between workload types

done

echo "\n========================================"
echo "All experiments completed!"
echo "Results saved in: $OUTPUT_DIR"
echo "========================================"