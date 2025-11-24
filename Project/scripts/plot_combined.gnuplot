set datafile separator ","

# Combined graph with throughput and response time
set term pngcairo size 1024,768 enhanced font 'Arial,12'
set output outfile

# Multiplot layout - two graphs stacked vertically
set multiplot layout 2,1 title "Workload Performance: " . workload_name

# First plot - Throughput
set title "Throughput vs Load Level"
set xlabel "Load Level (users)"
set ylabel "Throughput (req/s)"
set key off
set grid
plot infile using 1:2 with lines lw 3 linecolor "blue" title "Throughput"

# Second plot - Response Time
set title "Response Time vs Load Level"
set xlabel "Load Level (users)"
set ylabel "Response time (ms)"
set key off
set grid
plot infile using 1:3 with lines lw 3 linecolor "red" title "Response Time"

unset multiplot

# Print success message
print "Generated combined graph: ".outfile