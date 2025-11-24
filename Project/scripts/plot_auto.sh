#!/bin/bash
set -euo pipefail

# Automatic plotting script that works with any workload CSV
CSV_FILE=${1:-latencies.csv}
WORKLOAD_NAME=${2:-$(basename "$CSV_FILE" .csv | sed 's/_[^_]*$//')}

# Generate output filename
BASENAME=$(basename "$CSV_FILE" .csv)
OUTPUT_FILE="performance_${BASENAME}.png"

# Check if CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    echo "Error: CSV file '$CSV_FILE' not found!"
    echo "Usage: $0 [csv_file] [workload_name]"
    echo "Example: $0 latencies.csv put_all"
    exit 1
fi

# Generate combined graph using gnuplot
echo "Generating performance graph for workload: $WORKLOAD_NAME"
echo "Input file: $CSV_FILE"
echo "Output file: $OUTPUT_FILE"

gnuplot -e "infile='${CSV_FILE}';outfile='${OUTPUT_FILE}';workload_name='${WORKLOAD_NAME}'" plot_combined.gnuplot

echo "✅ Successfully generated: $OUTPUT_FILE"
echo "📊 Graph contains: Throughput + Response Time plots"