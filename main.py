import os
import subprocess
import yaml
import datetime
from pathlib import Path

CONFIG_PATH = 'configs/config.yaml'


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)


def check_pgbench_scale(conn_str, expected_scale):
    import psycopg2
    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM pgbench_accounts;")
        count = cur.fetchone()[0]
        conn.close()
        return count == expected_scale * 100000
    except Exception:
        return False


def run_pgbench(instance, bench, output_dir):
    results = []
    conn_str = instance['conn_str']
    name = instance['name']
    scale = bench['scale_factor']
    clients = bench['clients']
    threads = bench['threads']
    duration = bench['duration']
    reps = bench['repetitions']
    script = bench.get('script')

    # Check existing initialization
    need_init = True
    try:
        import psycopg2
        print(f"[INFO] Checking if pgbench_accounts table exists and matches scale_factor for {name}...")
        if check_pgbench_scale(conn_str, scale):
            resp = input(f"[PROMPT] pgbench_accounts table already matches scale_factor {scale} for {name}. Re-initialize? (y/N): ").strip().lower()
            if resp != 'y':
                need_init = False
    except ImportError:
        print("[WARN] psycopg2 not installed, skipping DB check. Always initializing.")
    except Exception as e:
        print(f"[WARN] Could not check pgbench_accounts table: {e}. Always initializing.")

    if need_init:
        print(f"[INFO] Initializing DB for {name} with scale {scale}...")
        subprocess.run([
            'pgbench', '-i', '-s', str(scale), conn_str
        ], check=True)
    else:
        print(f"[INFO] Skipping initialization for {name}.")

    import csv
    import re
    csv_file = os.path.join(
        output_dir, f"{name}_{bench['name']}_results.csv")
    fieldnames = [
        'instance', 'benchmark', 'run', 'scaling_factor', 'clients', 'threads', 'duration',
        'transactions', 'failed_transactions', 'latency_avg_ms', 'tps'
    ]
    rows = []
    for r in range(reps):
        print(f"[INFO] Running {bench['name']} benchmark (run {r+1}/{reps}) on {name}...")
        cmd = [
            'pgbench',
            '-c', str(clients),
            '-j', str(threads),
            '-T', str(duration)
        ]
        if bench.get('select_only', False):
            cmd.append('-S')
        cmd.append(conn_str)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        output = proc.stdout

        # Parse output
        def extract(pattern, text, cast=str, default=None):
            m = re.search(pattern, text)
            if m:
                return cast(m.group(1))
            return default
        transactions = extract(r"number of transactions actually processed: (\d+)", output, int)
        failed_transactions = extract(r"number of failed transactions: (\d+)", output, int)
        latency_avg = extract(r"latency average = ([\d.]+) ms", output, float)
        tps = extract(r"tps = ([\d.]+) ", output, float)
        row = {
            'instance': name,
            'benchmark': bench['name'],
            'run': r+1,
            'scaling_factor': scale,
            'clients': clients,
            'threads': threads,
            'duration': duration,
            'transactions': transactions,
            'failed_transactions': failed_transactions,
            'latency_avg_ms': latency_avg,
            'tps': tps
        }
        rows.append(row)
    # Write CSV
    with open(csv_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[INFO] Results written to {csv_file}")
    return csv_file


def main():
    config = load_config()
    ensure_dir(config['output_dir'])
    for instance in config['postgresql_instances']:
        for bench in config['benchmarks']:
            print(f"[INFO] Benchmarking {instance['name']} with {bench['name']} scenario...")
            run_pgbench(instance, bench, config['output_dir'])
    print("[DONE] All benchmarks completed.")


if __name__ == '__main__':
    main()
