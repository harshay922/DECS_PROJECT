#!/bin/bash
set -euo pipefail

# Script to generate a summary comparison graph of all workloads

WORKLOAD_TYPES=("put_all" "get_all" "get_popular" "mixed")
COLORS=("blue" "red" "green" "purple")
LINETYPES=("1" "2" "3" "4")

# Create summary CSV by combining all workload data
echo "Creating summary comparison data..."

# Create header for summary CSV
echo "threads,throughput,response_time,workload" > performance_summary.csv

for workload in "${WORKLOAD_TYPES[@]}"; do
    CSV_FILE="performance_${workload}.csv"
    if [ -f "$CSV_FILE" ]; then
        # Skip header and add workload type to each line
        tail -n +2 "$CSV_FILE" | awk -v w="$workload" '{print $0 "," w}' >> performance_summary.csv
    fi
done

# Generate summary comparison graph using gnuplot
echo "Generating summary comparison graph..."

gnuplot << EOF
set datafile separator ","
set term pngcairo size 1200,800 enhanced font 'Arial,12'
set output "performance_comparison.png"

# Multiplot layout - two graphs for comparison
set multiplot layout 2,1 title "Workload Performance Comparison"

# First plot - Throughput Comparison
set title "Throughput Comparison Across Workloads"
set xlabel "Thread Count"
set ylabel "Throughput (req/s)"
set key top left
set grid

plot \
EOF

# Add plot commands for each workload
for i in "${!WORKLOAD_TYPES[@]}"; do
    workload="${WORKLOAD_TYPES[$i]}"
    color="${COLORS[$i]}"
    linetype="${LINETYPES[$i]}"
    
    if [ $i -eq 0 ]; then
        echo -n "    'performance_summary.csv' using (\$1):(\$2):(\$4 == \"$workload\" ? \$2 : 1/0) with lines lw 2 linecolor $color lt $linetype title \"$workload\""
    else
        echo -n ", \\\n     'performance_summary.csv' using (\$1):(\$2):(\$4 == \"$workload\" ? \$2 : 1/0) with lines lw 2 linecolor $color lt $linetype title \"$workload\""
    fi
done

echo ""

# Second plot - Response Time Comparison
set title "Response Time Comparison Across Workloads"
set xlabel "Thread Count"
set ylabel "Response Time (ms)"
set key top left
set grid

plot \
EOF

# Add plot commands for each workload
for i in "${!WORKLOAD_TYPES[@]}"; do
    workload="${WORKLOAD_TYPES[$i]}"
    color="${COLORS[$i]}"
    linetype="${LINETYPES[$i]}"
    
    if [ $i -eq 0 ]; then
        echo -n "    'performance_summary.csv' using (\$1):(\$3):(\$4 == \"$workload\" ? \$3 : 1/0) with lines lw 2 linecolor $color lt $linetype title \"$workload\""
    else
        echo -n ", \\\n     'performance_summary.csv' using (\$1):(\$3):(\$4 == \"$workload\" ? \$3 : 1/0) with lines lw 2 linecolor $color lt $linetype title \"$workload\""
    fi
done

echo ""

unset multiplot
EOF

echo "✅ Summary comparison graph generated: performance_comparison.png"
echo "This graph shows throughput and response time for all workloads on the same scale!"