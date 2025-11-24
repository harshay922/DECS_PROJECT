set datafile separator ","

# Throughput graph
set term pngcairo size 1024,512
set output throughput_outfile
set title "Throughput vs Load Level"
set xlabel "Load Level (users)"
set ylabel "Throughput (req/s)"
set key off
plot infile using 1:2 with lines lw 2 title "Throughput"

# Response time graph  
set term pngcairo size 1024,512
set output latency_outfile
set title "Response Time vs Load Level"
set xlabel "Load Level (users)"
set ylabel "Response time (ms)"
set key off
plot infile using 1:3 with lines lw 2 title "Response Time"