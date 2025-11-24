#!/bin/bash
set -euo pipefail
CSV=${1:-results_get_popular.csv}
BASENAME=$(basename "$CSV" .csv)
THROUGHPUT_OUT="throughput_${BASENAME}.png"
LATENCY_OUT="latency_${BASENAME}.png"

gnuplot -e "infile='${CSV}';throughput_outfile='${THROUGHPUT_OUT}';latency_outfile='${LATENCY_OUT}'" plot_gnuplot.gnuplot

echo "Generated graphs:"
echo "Throughput: ${THROUGHPUT_OUT}"
echo "Latency: ${LATENCY_OUT}"