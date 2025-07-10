# pgbench Orchestrator

This tool automates repeated benchmarking of PostgreSQL instances using pgbench for disk IO and CPU intensive scenarios.

## Requirements
- Python 3.8+
- `pgbench` must be installed and available in your PATH
- `pyyaml` Python package

## Usage
1. Edit `configs/config.yaml` to set your PostgreSQL connection strings and benchmark parameters.
2. Run the orchestrator:
   ```bash
   python main.py
   ```
3. Results will be saved in the `results/` directory.

## Benchmark Scenarios
- **Disk IO Intensive:** Standard pgbench workload with a very large scale factor (20000), making the dataset much larger than RAM, ensuring true disk IO pressure.
- **CPU Intensive:** Built-in pgbench 'select-only' mode (`-S`) for CPU stress with minimal disk IO.

You can adjust parameters in the config file for your needs.
