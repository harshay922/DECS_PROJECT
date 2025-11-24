#!/bin/bash
set -euo pipefail

# Script to run tests for ALL workload types and generate graphs for each

SERVER_URL="http://localhost:8080"
DURATION=10
KEYSPACE=10000
POPULAR_KEYS=1000

# Workload types to test
WORKLOAD_TYPES=("put_all" "get_all" "get_popular" "mixed")

# Thread counts to test
THREAD_COUNTS=(2 4 6 8 10 12 16)

echo "🚀 Running comprehensive performance tests for ALL workload types..."
echo "Workloads: ${WORKLOAD_TYPES[*]}"
echo "Thread counts: ${THREAD_COUNTS[*]}"
echo ""

for workload in "${WORKLOAD_TYPES[@]}"; do
    echo "==============================================="
    echo "🧪 Testing workload: $workload"
    echo "==============================================="
    
    # Create workload-specific CSV file
    CSV_FILE="performance_${workload}.csv"
    rm -f "$CSV_FILE"
    
    for threads in "${THREAD_COUNTS[@]}"; do
        echo "📊 Testing $workload with $threads threads..."
        cd ../src
        
        if [ "$workload" = "mixed" ]; then
            # For mixed workload, add the ratio parameters
            ./loadgen "$SERVER_URL" "$threads" "$DURATION" "$workload" "$KEYSPACE" "$POPULAR_KEYS" "0.3" "0.6" "0.1"
        else
            ./loadgen "$SERVER_URL" "$threads" "$DURATION" "$workload" "$KEYSPACE" "$POPULAR_KEYS"
        fi
        
        cd ../scripts
        echo ""
    done
    
    # Generate graph for this workload
    echo "🖼️  Generating performance graph for $workload..."
    ./plot_auto.sh "$CSV_FILE" "$workload"
    
    echo "✅ Completed testing for $workload"
    echo "📊 Graph saved: performance_${workload}.png"
    echo ""
    
    # Wait a moment between workloads to let server stabilize
    sleep 2
done

echo "==============================================="
echo "🎉 ALL WORKLOAD TESTS COMPLETED!"
echo "==============================================="
echo ""
echo "📈 Performance graphs generated for all workloads:"
for workload in "${WORKLOAD_TYPES[@]}"; do
    echo "   • performance_${workload}.png - $workload workload"
done
echo ""
echo "Each graph shows:"
echo "   • Throughput (blue line) vs Thread Count"
echo "   • Response Time (red line) vs Thread Count"
echo ""
echo "You can now compare performance across different workload types!"

# Create a summary comparison graph
echo ""
echo "📊 Generating summary comparison graph..."
./generate_summary_graph.sh

echo "✅ Summary graph: performance_comparison.png"