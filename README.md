# OFICrossImpact

This repository contains a partial implementation of the paper [Cross-impact of order flow imbalance in equity
markets](https://doi.org/10.1080/14697688.2023.2236159) by Cont et. al.

### Running the code

1. Install requirements as listed in `requirements.txt`
2. In `data/raw`, place `csv.zstd` files in MBP-10 format under directories titled with the respective stock IDs (e.g. TSLA, AAPL etc.)
3. For each desired stock, run `python scripts/order_flow.py --stock_id YOUR_STOCK --output_path data/processed` to generate minute-aggregated order flow imbalance values as per the paper.
4. Generate cross-impact analysis plots by running `notebooks/cross_impact_analysis.ipynb`.

### Data used

The plots visible here were generated using NASDAQ TotalItch data from 10-07-2024 to 10-11-2024, retrieved from DataBento.