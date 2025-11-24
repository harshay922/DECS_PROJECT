#!/usr/bin/env python3

import json
import glob
import re
import os
import csv
from datetime import datetime

# Import required data analysis libraries
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def parse_loadgen_output(filename):
    """Parse load generator output file"""
    results = {
        'throughput': 0,
        'p50_latency': 0,
        'p90_latency': 0, 
        'p95_latency': 0,
        'p99_latency': 0,
        'total_ops': 0,
        'errors': 0,
        'cache_hit_ratio': 0.0,
        'cache_evictions': 0,
        'cpu_usage': 0.0,
        'memory_usage': 0,
        'network_throughput': 0.0
    }
    
    try:
        with open(filename, 'r') as f:
            content = f.read()
            
        # Parse throughput
        throughput_match = re.search(r'Throughput: ([\d.]+) ops/sec', content)
        if throughput_match:
            results['throughput'] = float(throughput_match.group(1))
            
        # Parse latency percentiles
        latency_match = re.search(r'P50=([\d.]+)ms\s+P90=([\d.]+)ms\s+P95=([\d.]+)ms\s+P99=([\d.]+)ms', content)
        if latency_match:
            results['p50_latency'] = float(latency_match.group(1))
            results['p90_latency'] = float(latency_match.group(2))
            results['p95_latency'] = float(latency_match.group(3))
            results['p99_latency'] = float(latency_match.group(4))
            
        # Parse total operations and errors
        ops_match = re.search(r'Total Ops: (\d+)', content)
        if ops_match:
            results['total_ops'] = int(ops_match.group(1))
            
        errors_match = re.search(r'Errors: (\d+)', content)
        if errors_match:
            results['errors'] = int(errors_match.group(1))
            
        # Parse cache statistics
        cache_hit_match = re.search(r'Hit Ratio: ([\d.]+)%', content)
        if cache_hit_match:
            results['cache_hit_ratio'] = float(cache_hit_match.group(1)) / 100.0
            
        cache_evictions_match = re.search(r'Evictions: (\d+)', content)
        if cache_evictions_match:
            results['cache_evictions'] = int(cache_evictions_match.group(1))
            
        # Parse server statistics
        cpu_match = re.search(r'CPU Usage: ([\d.]+)%', content)
        if cpu_match:
            results['cpu_usage'] = float(cpu_match.group(1))
            
        memory_match = re.search(r'Memory Usage: (\d+)MB', content)
        if memory_match:
            results['memory_usage'] = int(memory_match.group(1))
            
        network_match = re.search(r'Network Throughput: ([\d.]+) MB/s', content)
        if network_match:
            results['network_throughput'] = float(network_match.group(1))
            
    except Exception as e:
        print(f"Error parsing {filename}: {e}")
    
    return results

def parse_stats_json(filename):
    """Parse server stats JSON file"""
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Error parsing {filename}: {e}")
        return {}

def parse_resource_logs(cpu_file, disk_file, network_file):
    """Parse resource monitoring logs"""
    resources = {'cpu': 0, 'memory': 0, 'disk_read': 0, 'disk_write': 0, 'network': 0}
    
    try:
        # Parse CPU/Memory (top output)
        if cpu_file:
            with open(cpu_file, 'r') as f:
                cpu_lines = f.readlines()
                cpu_values = []
                for line in cpu_lines:
                    if 'server' in line and '%CPU' in line:
                        parts = line.split()
                        if len(parts) > 8:
                            cpu_values.append(float(parts[8]))
                if cpu_values:
                    resources['cpu'] = np.mean(cpu_values)
    
        # Parse Disk I/O (iostat output)
        if disk_file:
            with open(disk_file, 'r') as f:
                disk_lines = f.readlines()
                # Simplified parsing - adjust based on your iostat output
                pass
                
    except Exception as e:
        print(f"Error parsing resource logs: {e}")
    
    return resources

def main():
    """Main analysis function"""
    
    data = []
    
    # Process put_all workload with 10 threads
    result_file = "results/put_all_10th.txt"
    if os.path.exists(result_file):
        # Parse results
        loadgen_results = parse_loadgen_output(result_file)
        
        # Create experiment data from parsed results
        experiment_data = {
            'workload': 'put_all',
            'threads': 10,
            'throughput': loadgen_results['throughput'],
            'p50_latency': loadgen_results['p50_latency'],
            'p90_latency': loadgen_results['p90_latency'],
            'p95_latency': loadgen_results['p95_latency'],
            'p99_latency': loadgen_results['p99_latency'],
            'total_ops': loadgen_results['total_ops'],
            'errors': loadgen_results['errors'],
            'cache_hit_ratio': loadgen_results['cache_hit_ratio'],
            'cache_evictions': loadgen_results['cache_evictions'],
            'cpu_usage': loadgen_results['cpu_usage'],
            'memory_usage': loadgen_results['memory_usage'],
            'network_throughput': loadgen_results['network_throughput']
        }
        
        data.append(experiment_data)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        print("No experiment data found!")
        return
    
    # Save to CSV
    df.to_csv('analysis_results.csv', index=False)
    print("Analysis results saved to analysis_results.csv")
    
    # Generate plots
    generate_plots(df)

def generate_plots(df):
    """Generate performance plots"""
    
    # Create a simple plot for single experiment
    plt.figure(figsize=(12, 8))
    
    # Throughput plot
    plt.subplot(2, 2, 1)
    plt.bar(['Throughput'], [df['throughput'].iloc[0]])
    plt.ylabel('Throughput (ops/sec)')
    plt.title('Throughput Performance')
    plt.grid(True, alpha=0.3)
    
    # Latency plot
    plt.subplot(2, 2, 2)
    latencies = ['P50', 'P90', 'P95', 'P99']
    values = [df['p50_latency'].iloc[0], df['p90_latency'].iloc[0], 
              df['p95_latency'].iloc[0], df['p99_latency'].iloc[0]]
    plt.bar(latencies, values)
    plt.ylabel('Latency (ms)')
    plt.title('Latency Percentiles')
    plt.grid(True, alpha=0.3)
    
    # Cache performance
    plt.subplot(2, 2, 3)
    if 'cache_hit_ratio' in df.columns:
        plt.bar(['Hit Ratio'], [df['cache_hit_ratio'].iloc[0] * 100])
        plt.ylabel('Cache Hit Ratio (%)')
        plt.title('Cache Performance')
        plt.grid(True, alpha=0.3)
    
    # Operations summary
    plt.subplot(2, 2, 4)
    plt.bar(['Total Ops'], [df['total_ops'].iloc[0]])
    plt.ylabel('Operations')
    plt.title('Total Operations')
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('performance_summary.png', dpi=300, bbox_inches='tight')
    
    # Create latency distribution from CSV file
    try:
        latency_data = pd.read_csv('results/put_all_10th_latencies.csv')
        plt.figure(figsize=(10, 6))
        plt.hist(latency_data['latency_ms'], bins=50, alpha=0.7, edgecolor='black')
        plt.xlabel('Latency (ms)')
        plt.ylabel('Frequency')
        plt.title('Latency Distribution - put_all workload (10 threads)')
        plt.grid(True, alpha=0.3)
        plt.savefig('latency_distribution.png', dpi=300, bbox_inches='tight')
    except Exception as e:
        print(f"Could not create latency distribution plot: {e}")
    
    print("Performance plots generated successfully!")

if __name__ == "__main__":
    main()