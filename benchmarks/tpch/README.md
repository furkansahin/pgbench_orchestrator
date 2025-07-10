# TPC-H Benchmark Integration

This directory is intended for TPC-H tooling (dbgen, queries, scripts) to be used by the orchestrator.

## How it will work
- We will use `dbgen` to generate TPC-H data at a chosen scale factor.
- The orchestrator will load this data into PostgreSQL.
- The orchestrator will run the standard TPC-H queries (1-22) and collect timing/results.
- Results will be parsed and aggregated into a CSV, similar to pgbench.

## Requirements
- `dbgen` (compiled for Linux, available in this directory)
- TPC-H query `.sql` files
- Python requirements: `psycopg2`, `pandas`, `pyyaml`

## Next Steps
- Add dbgen and queries
- Implement orchestration logic in main.py
