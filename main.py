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


def run_pgbench(instance, bench, output_dir, skip_db_check=False):
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
    if not skip_db_check:
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
    else:
        print("[INFO] Skipping DB check due to --skip-db-check flag. Always initializing.")

    if need_init and not skip_db_check:
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


def run_tpch(instance, bench, output_dir):
    import csv
    import glob
    import time
    import psycopg2
    import re
    conn_str = instance['conn_str']
    name = instance['name']
    scale = bench['scale_factor']
    reps = bench['repetitions']
    queries_dir = bench.get('queries_dir')
    tpch_dir = os.path.join(os.path.dirname(__file__), 'benchmarks', 'tpch')
    dbgen_dir = os.path.join(tpch_dir, 'dbgen')
    dbgen_bin = os.path.join(dbgen_dir, 'dbgen')
    ddl_path = os.path.join(tpch_dir, 'tpch_postgres_ddl.sql')
    load_path = os.path.join(tpch_dir, 'load_tpch_data.sql')
    csv_file = os.path.join(output_dir, f"{name}_tpch_results.csv")
    fieldnames = ['instance', 'query', 'run', 'scale_factor', 'elapsed_sec', 'rowcount']
    rows = []

    # 1. Generate data with dbgen
    print(f"[INFO] Generating TPC-H data with dbgen at scale {scale}...")
    subprocess.run([dbgen_bin, '-f', '-s', str(scale)], cwd=dbgen_dir, check=True)

    # 2. Create tables
    print(f"[INFO] Creating TPC-H tables...")
    subprocess.run(['psql', conn_str, '-f', ddl_path], check=True)

    # 3. Load data into tables
    print(f"[INFO] Loading TPC-H data into tables...")
    subprocess.run(['psql', conn_str, '-f', load_path], cwd=dbgen_dir, check=True)

    # 4. Run queries
    query_files = sorted(glob.glob(os.path.join(queries_dir, '*.sql')), key=lambda x: int(re.findall(r'(\d+)', os.path.basename(x))[0]))
    # Load query parameters
    import yaml as pyyaml
    params_path = os.path.join(tpch_dir, 'query_parameters.yaml')
    if os.path.exists(params_path):
        with open(params_path) as pf:
            param_map = pyyaml.safe_load(pf)
    else:
        param_map = {}

    for r in range(reps):
        for qf in query_files:
            qname = os.path.basename(qf)
            qnum = int(re.findall(r'(\d+)', qname)[0])
            with open(qf) as f:
                query = f.read()
            # Substitute parameters :1, :2, ... if present
            params = param_map.get(qnum, [])
            for idx, val in enumerate(params, 1):
                # If value is string and not already quoted, quote it for SQL
                if isinstance(val, str) and not val.startswith("'"):
                    val = f"'{val}'"
                query = query.replace(f":{idx}", str(val))
            print(f"[INFO] Running TPC-H query {qname} (run {r+1}/{reps})...")
            try:
                start = time.time()
                with psycopg2.connect(conn_str) as conn:
                    with conn.cursor() as cur:
                        cur.execute(query)
                        try:
                            rowcount = cur.rowcount
                        except Exception:
                            rowcount = None
                elapsed = time.time() - start
                rows.append({
                    'instance': name,
                    'query': qname,
                    'run': r+1,
                    'scale_factor': scale,
                    'elapsed_sec': elapsed,
                    'rowcount': rowcount
                })
            except Exception as e:
                print(f"[ERROR] Query {qname} failed: {e}")
                rows.append({
                    'instance': name,
                    'query': qname,
                    'run': r+1,
                    'scale_factor': scale,
                    'elapsed_sec': None,
                    'rowcount': None
                })
    # Write CSV
    with open(csv_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[INFO] TPC-H results written to {csv_file}")
    return csv_file


def main():
    import argparse
    parser = argparse.ArgumentParser(description="pgbench orchestrator")
    parser.add_argument('--skip-db-check', action='store_true', help='Skip pgbench_accounts table check and always initialize')
    args = parser.parse_args()

    config = load_config()
    ensure_dir(config['output_dir'])
    for instance in config['postgresql_instances']:
        first = True
        for bench in config['benchmarks']:
            print(f"[INFO] Benchmarking {instance['name']} with {bench['name']} scenario...")
            if bench['name'] == 'tpch':
                run_tpch(instance, bench, config['output_dir'])
            else:
                run_pgbench(instance, bench, config['output_dir'], skip_db_check=(args.skip_db_check or not first))
            first = False
    print("[DONE] All benchmarks completed.")


if __name__ == '__main__':
    main()
