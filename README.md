# Jaffle Shop dlt Pipeline (Optimized + Automated via GitHub Actions)

This repository contains a high‑performance **dlt** pipeline that extracts data from the public **Jaffle Shop API** and loads it into **DuckDB**.  
It is fully automated using **GitHub Actions**, enabling scheduled runs, CI/CD, and reproducible data ingestion.

The pipeline is optimized for maximum throughput using:

- **Parallel extraction** (customers + orders downloaded simultaneously)
- **Aggressive chunking** (5,000‑row batches for fast normalization)
- **Worker tuning** (4 extract, 4 normalize, 8 load workers)
- **Buffer control** (large buffers to reduce I/O overhead)
- **File rotation** (more files → more parallel load workers → faster ingestion)

The API used is the one documented at: https://jaffle-shop.scalevector.ai/docs

