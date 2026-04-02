#!/usr/bin/env python3
"""Quick cross-algorithm comparison at fixed data_size and parallelism levels."""

import subprocess, csv, json, io, os, sys, time

BINARY = "build/bin/test_join_baseline_integration"
OUTPUT_DIR = "test/result/integration"

# We want: bruteforce, ivf, vsjoin at same configs
# Use existing test cases that have comparable sizes
TESTS = [
    ("bruteforce_baseline", [1, 4]),
    ("ivf_standard", [1, 4]),
    ("vsjoin_baseline", [1, 4]),
    ("clustered_k_sweep_k4", [4]),
]

def run_test(test_name, env_extra=None):
    """Run a single test case and return output."""
    env = os.environ.copy()
    env["SAGEFLOW_VSJOIN_DEBUG_SUBTASK"] = "1"
    if env_extra:
        env.update(env_extra)
    
    filter_str = f"*{test_name}*"
    cmd = [BINARY, f"--gtest_filter={filter_str}"]
    
    print(f"Running {test_name}...", flush=True)
    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, env=env,
                           cwd="/root/sageFlow")
    elapsed = time.time() - start
    print(f"  Done in {elapsed:.1f}s (exit={result.returncode})", flush=True)
    return result.stdout + result.stderr

def parse_csv_results(test_name):
    """Parse the CSV result file."""
    csv_path = os.path.join("/root/sageFlow", OUTPUT_DIR, f"{test_name}_results.csv")
    if not os.path.exists(csv_path):
        return []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        return list(reader)

def extract_subtask_stats(output):
    """Extract VSJOIN_SUBTASK lines."""
    lines = []
    for line in output.split('\n'):
        if 'VSJOIN_SUBTASK' in line:
            lines.append(line.strip())
    return lines

def extract_breakdown(output):
    """Extract breakdown metrics from log."""
    lines = []
    for line in output.split('\n'):
        if 'Breakdown(us)' in line or 'breakdown' in line.lower():
            lines.append(line.strip())
    return lines

def main():
    os.chdir("/root/sageFlow")
    
    all_results = {}
    subtask_data = {}
    
    for test_name, _ in TESTS:
        output = run_test(test_name)
        results = parse_csv_results(test_name)
        all_results[test_name] = results
        subtask_data[test_name] = extract_subtask_stats(output)
        
        # Also look for breakdown lines
        breakdown_lines = extract_breakdown(output)
        if breakdown_lines:
            print(f"  Breakdown for {test_name}:")
            for l in breakdown_lines[-3:]:
                print(f"    {l}")
    
    # Print comparison table
    print("\n" + "="*120)
    print("CROSS-ALGORITHM COMPARISON")
    print("="*120)
    print(f"{'Algorithm':<20} {'Size':<6} {'P':<4} {'Recall':<8} {'Precision':<10} {'ExecTime(ms)':<14} {'JoinTime(ms)':<14} {'Throughput':<12} {'SinkWait(ms)':<12} {'Dedup':<10}")
    print("-"*120)
    
    for test_name, _ in TESTS:
        for r in all_results.get(test_name, []):
            algo = r.get('algorithm', '?')
            size = r.get('data_size', '?')
            p = r.get('parallelism', '?')
            recall = r.get('recall', '?')
            prec = r.get('precision', '?')
            exec_ms = r.get('execution_time_ms', '?')
            join_ms = r.get('join_time_ms', '?')
            tput = r.get('throughput_rps', '?')
            sink_wait = r.get('sink_wait_ms', '?')
            dedup = r.get('sink_dedup', '?')
            passed = r.get('passed', '?')
            marker = "✓" if passed == "true" else "✗"
            print(f"{algo:<20} {size:<6} {p:<4} {recall:<8} {prec:<10} {exec_ms:<14} {join_ms:<14} {tput:<12} {sink_wait:<12} {dedup:<10} {marker}")
    
    # Print subtask load info
    print("\n" + "="*80)
    print("VSJOIN SUBTASK LOAD DISTRIBUTION")
    print("="*80)
    for test_name in ["vsjoin_baseline"]:
        if subtask_data.get(test_name):
            for line in subtask_data[test_name]:
                print(f"  {line}")
        else:
            print(f"  No subtask data for {test_name}")
    
    print("\nDone.")

if __name__ == "__main__":
    main()
