#!/bin/bash

# Resource monitoring script for CS744 Project
# Usage: ./monitor_resources.sh <duration_seconds> <output_prefix>

DURATION=$1
PREFIX=$2
SERVER_PID=$(pgrep -f "server")

if [ -z "$SERVER_PID" ]; then
    echo "Server process not found!"
    exit 1
fi

echo "Monitoring server PID: $SERVER_PID for $DURATION seconds"

# CPU and Memory monitoring
top -b -d 1 -p $SERVER_PID -n $DURATION > "${PREFIX}_cpu_memory.log" &
TOP_PID=$!

# Disk I/O monitoring (use vmstat if iostat not available)
if command -v iostat &> /dev/null; then
    iostat -x 1 $DURATION > "${PREFIX}_disk_io.log" &
    IOSTAT_PID=$!
else
    echo "iostat not available, using vmstat for disk I/O monitoring"
    vmstat 1 $DURATION > "${PREFIX}_disk_io.log" &
    IOSTAT_PID=$!
fi

# Network monitoring (use netstat if iftop not available)
if command -v iftop &> /dev/null; then
    iftop -t -s 1 -n -N -L 1000 2>/dev/null > "${PREFIX}_network.log" &
    IFTOP_PID=$!
else
    echo "iftop not available, using netstat for network monitoring"
    netstat -s 1 > "${PREFIX}_network.log" &
    IFTOP_PID=$!
fi

# Wait for monitoring duration
sleep $DURATION

# Kill monitoring processes
kill $TOP_PID $IOSTAT_PID $IFTOP_PID 2>/dev/null

echo "Resource monitoring completed. Files saved:"
echo "- ${PREFIX}_cpu_memory.log"
echo "- ${PREFIX}_disk_io.log" 
echo "- ${PREFIX}_network.log"