#!/bin/bash
set -euo pipefail

# Script to run multiple load tests with different thread counts
# This will generate proper line graphs with multiple data points

SERVER_URL="http://localhost:8080"
DURATION=10
WORKLOAD_TYPE="get_popular"
KEYSPACE=10000
POPULAR_KEYS=1000

# Clear previous performance data
echo "🧹 Clearing previous performance data..."
rm -f performance_data.csv

# Thread counts to test
THREAD_COUNTS=(2 4 6 8 10 12 16)

echo "🚀 Running multiple load tests to generate performance data..."
echo "Workload: $WORKLOAD_TYPE"
echo "Thread counts: ${THREAD_COUNTS[*]}"
echo ""

for threads in "${THREAD_COUNTS[@]}"; do
    echo "📊 Testing with $threads threads..."
    cd ../src
    ./loadgen "$SERVER_URL" "$threads" "$DURATION" "$WORKLOAD_TYPE" "$KEYSPACE" "$POPULAR_KEYS"
    cd ../scripts
    echo ""
done

echo "✅ All tests completed!"
echo "📈 Performance data saved to: performance_data.csv"
echo ""

# Generate the final graph
echo "🖼️  Generating final performance graph..."
./plot_auto.sh performance_data.csv "$WORKLOAD_TYPE"

echo ""
echo "🎉 Done! Check performance_${WORKLOAD_TYPE}.png for your line graphs!"
echo "The graph shows how throughput and response time scale with different thread counts."