# 🚖 OpsPulse-NYC-Taxi-Pipeline
A modular ETL pipeline that extracts, transforms, and analyzes NYC taxi trip data.

## 📥 Data Ingestion & Transformation

####  What Data is Ingested?

The pipeline ingests **NYC Yellow Taxi Trip data** in Parquet format from a public dataset.

This dataset includes:

* Pickup and drop-off timestamps
* Trip distance
* Fare amount and total amount
* Payment type
* Pickup and drop-off locations


#### Why This Data Exists?

This dataset is published to:

* Provide transparency into NYC taxi operations
* Enable data analysis for transportation trends
* Support research and business decision-making
* Help optimize traffic, pricing, and city planning


#### How the Data is Transformed

After extraction, the raw data goes through several transformation steps:

* **Data Cleaning**

  * Remove duplicate records
  * Handle missing or invalid values

* **Feature Engineering**

  * Extract pickup day from timestamp
  * Create new fields for analysis

* **Aggregation**

  * Calculate **revenue per day**
  * Generate business-level insights

* **Validation**

  * Ensure correct data types
  * Filter out invalid records


#### Output

The transformed data is stored in:

* `data/processed/` → Cleaned dataset
* `data/insights/` → Aggregated metrics (e.g., revenue per day)

This enables easy analysis and reporting.


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

## 🛠️ Setup Instructions

### 1. Clone Repository

```bash
git clone https://github.com/your-username/OpsPulse-NYC-Taxi-Pipeline.git
```

```bash
cd OpsPulse-NYC-Taxi-Pipeline
```

---

### 2. Create Virtual Environment

```bash
uv venv
```

---

### 3. Activate Environment (Windows)

```cmd
.venv\Scripts\activate
```

---

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### Run the Pipeline

```bash
python scripts/main.py
```

