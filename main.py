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

    for r in range(reps):
        ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        out_file = os.path.join(
            output_dir, f"{name}_{bench['name']}_run{r+1}_{ts}.txt")
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
        with open(out_file, 'w') as f:
            proc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
        results.append(out_file)
    return results


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
