# Job Post Scraper

### Automated Job Market Pipeline for Climate Risk & Data Science

**Author:** Andrew Gorin, PhD

## The Problem
The modern job hunt is a "Signal-to-Noise" problem. High-quality roles in **Climate Risk**, **Data Science**, and **Technical Consulting** are often buried under thousands of low-relevance listings on major job boards. Manually filtering these platforms introduces high latency and cognitive load.

##  The Solution
I treated the job search like a data engineering problem. This Python utility serves as a daily ETL (Extract, Transform, Load) pipeline that:
1.  **Extracts** fresh listings (last 24 hours) from LinkedIn, Indeed, and Glassdoor simultaneously.
2.  **Transforms** the raw feed using strict Boolean logic to filter for specific high-value skill combinations (e.g., `"Wildfire" + "Python" + "Stochastic Modeling"`).
3.  **Loads** the clean data into a CSV for rapid daily analysis.

This tool reduces my daily search latency, allowing me to focus on applications rather than scrolling.

## Technical Approach
The script utilizes `python-jobspy` to handle the scraping logic and `pandas` for data structuring. It segments the market into two specific tiers:

* **Tier 1 (Core Focus):** Climate Risk Modeling, Fraud/Decision Science, Environmental Consulting (EPA/NEPA).
* **Tier 2 (High Technical Upside):** Quant Energy Trading, Carbon MRV (Remote Sensing), Reinsurance Analytics.

## 🚀 Quick Start

### Prerequisites
* Python 3.9+

### Installation
1. Clone the repo:
   ```bash
   git clone [https://github.com/dgorin1/market-signal-detector.git](https://github.com/dgorin1/market-signal-detector.git)

2. Install dependencies:
```bash
pip install -r requirements.txt

### Usage
Run the script to generate today's market scan:
```bash
python job_hunter.py


### Output
The script generates a timestamped CSV file (e.g., `market_scan_2025-12-03.csv`) containing only the jobs that passed the Boolean filters.
