# 🚖 OpsPulse-NYC-Taxi-Pipeline
A modular ETL pipeline that extracts, transforms, and analyzes NYC taxi trip data.

## 📌 Project Overview

This project demonstrates a complete ETL (Extract, Transform, Load) workflow:
- Extracts NYC taxi data from a public dataset for prompted year and month.
- Transforms raw data into meaningful insights.
- Loads processed data for analysis.
  
Built with a focus on:

- Clean architecture
- Robust error handling
- Logging and observability
- Production-style pipeline design

## ⚙️ Tech Stack

- Python
- Pandas
- Requests
- Logging
- Parquet

## 📂 Project Structure

```
OpsPulse-NYC-Taxi-Pipeline/
│
├── data/
│   ├── raw/              # Raw downloaded parquet files
│   ├── processed/        # Cleaned/processed data
│   └── insights/         # Aggregated outputs (e.g. revenue per day)
│
├── logs/                 # Log files generated during pipeline execution
│
├── notebooks/            # Jupyter notebooks for exploration & analysis
│
├── scripts/
│   ├── extract.py        # Data extraction logic
│   ├── transform.py      # Data transformation logic
│   ├── load.py           # Data loading logic
│   ├── main.py           # Pipeline orchestrator
│   ├── config.py         # Configuration variables (paths, URLs)
│   └── logger.py         # Logging setup
│
├── requirements.txt
└── README.md
```
## 🚀 How It Works

### 1. Extract
- Downloads NYC taxi data (Parquet format)
- Handles HTTP errors (403, 404)
- Logs download activity
  
### 2. Transform
- Cleans data
- Removes duplicates
- Performs aggregations (e.g., revenue per day)
  
### 3. Load
- Stores processed data for analysis in JSON format.
  
### 4. Orchestration
- `main.py` controls the pipeline
- Implements fail-fast strategy
- Stops execution if any step fails
