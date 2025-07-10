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

    # Initialize DB if needed
    print(f"[INFO] Initializing DB for {name} with scale {scale}...")
    subprocess.run([
        'pgbench', '-i', '-s', str(scale), conn_str
    ], check=True)

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
