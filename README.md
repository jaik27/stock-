# NSE/MCX/BSE Data Pipeline (Windows Edition)

This project is a full end-to-end pipeline for ingesting, storing, processing, validating, and preparing Indian market data (NSE/MCX/BSE) for quant trading and analytics, **orchestrated by running `app.py`**.  
**All components, including schema creation and historical data import, are mandatory and run automatically on startup.**  
Designed for **Windows 10/11**.

---

## 🚦 System Flow

**ALL steps are managed by `app.py` in this exact order:**

1. **Schema Initialization** (`schema_design.py`)
   - Verifies and creates all required tables and views.
   - This script creates the PostgreSQL/TimescaleDB schema with the following tables:
     - `instruments`: Stores information about BankNifty and its options
     - `candle_data_1min`: 1-minute OHLC, volume, and open interest data
     - `candle_data_15sec`: 15-second data for the first 2 hours of trading
     - `technical_indicators`: Technical indicators (TVI, OBV, RSI, PVI, PVT)
     - `trading_signals`: Trading signals for ML/LLM applications

2. **Historical Data Import** (via `historical_downloader.py`)
   - Downloads and imports historical data before live collection starts.
   - Fully automatic on startup; no separate step required.

3. **Data Collection** (`data_collector.py`)
   - Downloads tickers.
   - Connects to the vendor websocket/API.
   - Subscribes to all relevant tickers (up to symbol limit, with filtering).
   - Writes live tick/candle data to the database.
   - Processes tick data into 1-minute and 15-second candles.
   - Stores data in TimescaleDB with 7-day retention policy.

4. **Indicator Calculation** (`indicator_calculator.py`)
   - Loads recent candle data.
   - Computes technical indicators:
     - On-Balance Volume (OBV)
     - Relative Strength Index (RSI)
     - Trend Volume Index (TVI)
     - Positive Volume Index (PVI)
     - Price Volume Trend (PVT)
   - Inserts results into the `technical_indicators` table.

5. **Pipeline Validation** (`pipeline_validator.py`)
   - Ensures all tables and views exist.
   - Validates that both recent and historical data are present and correct.
   - Checks indicator completeness and data retention.
   - Verifies timezones, technical indicator coverage, segment subscription, and more.

6. **Data Preparation** (`data_preparation.py`)
   - Prepares and exports data for ML/trading, including:
     - ML feature engineering and training dataset splitting
     - LLM/market summary data using **all available historical data**
     - Export of sample scalping data and signals
   - Handles NaN/inf cleaning and logs if no data is available.

**No step is optional.  
Do not attempt to run any component directly; always use `app.py`.**

---

## 🗂️ File Structure

```
.
├── app.py                     # Main orchestrator, run this!
├── schema_design.py           # Critical: schema + historical data loader (always runs first)
├── historical_downloader.py   # Batch import for historical data (auto-run by app.py)
├── data_collector.py
├── indicator_calculator.py
├── pipeline_validator.py
├── data_preparation.py
├── requirements.txt
├── .env
└── ...
```

---

## 🔧 Requirements

- **Windows 10/11**
- **Python 3.9+**
- **PostgreSQL 13+** (with optional TimescaleDB)
- **Vendor API credentials** (NSE/BSE/MCX websocket & REST)

---

## ⚙️ Setup

### 1. Python & Virtual Environment

```powershell
git clone <your-repo-url>
cd <repo-folder>
python -m venv venv
.\venv\Scripts\activate

pip install --upgrade pip
pip install -r requirements.txt
pip install psycopg2-binary pandas python-dotenv requests websocket-client numpy scikit-learn matplotlib
```

### 2. Database (PostgreSQL) Setup

- Install PostgreSQL and TimescaleDB.
- Create your database and run:

```sql
CREATE DATABASE nse_db;
\c nse_db
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

### 3. Configuration

Copy `.env.example` to `.env` and fill all required fields with your actual details:

```env
NSE_LOGIN_ID=DC-BPRA9112
NSE_PRODUCT=DIRECTRTLITE
NSE_API_KEY=your_actual_api_key_here
NSE_AUTH_ENDPOINT=http://116.202.165.216/api/gettoken
NSE_TICKERS_ENDPOINT=http://116.202.165.216/api/gettickers
NSE_WEBSOCKET_ENDPOINT=ws://116.202.165.216:992/directrt/
DB_NAME=nse_db
DB_USER=postgres
DB_PASSWORD=your_postgres_password
DB_HOST=localhost
DB_PORT=5432
```

---

## ▶️ Start the Entire Pipeline

**Always run:**

```powershell
python app.py
```

- This will:
  - Initialize and validate the schema (including required tables/views).
  - **Pull/import historical data automatically** (before live streaming starts).
  - Start the data collector, indicator calculation, pipeline validation, and data preparation—all in the correct order.

**Do NOT run component scripts directly. Only use `app.py`.**

---

## 📝 Important Notes (from your logs & instructions)

- **schema_design.py runs FIRST and is critical.**
- **Historical data import is always included and automatic.**
- **All tables/views:**  
  `instruments`, `candle_data_1min`, `candle_data_15sec`, `technical_indicators`, `trading_signals`, `first_two_hours_data`
- **TimescaleDB retention:**  
  If you see errors about missing `_timescaledb_config.bgw_policy_drop_chunks`, check TimescaleDB install or skip retention validation if not using it.
- **Timezone:**  
  Always store/process all datetimes as UTC and timezone-aware.
- **MCX/OPTCOM subscription errors:**  
  Vendor segment limit is enforced; filter out segments with a limit of 0 in `filter_relevant_tickers()` in `data_collector.py`.
- **15s candle data:**  
  If vendor doesn't supply 15s candles, edit the pipeline and validation to skip or ensure this data is present.
- **Technical indicators:**  
  Ensure the indicator calculator is running and writing to DB.
- **Log filtering:**  
  Logs for "Max Symbol Limit Reached" are automatically filtered out for clarity.
- **LLM/market summary:**  
  Data preparation now always uses all available historical data, with NaN/inf cleaning and logging when no data is available.
- **Start everything using `app.py`.**

---

## 🧑‍💻 Running as a Service on Windows

- Use Windows Task Scheduler or NSSM to run `app.py` as a background service.
- Always activate your venv in the task's startup command.

---

## ❓ FAQ

- **Q: How do I start the whole system?**  
  **A:** Run `python app.py` in your activated virtual environment. This always initializes the schema and pulls historical data before any other step.
- **Q: Why are MCX symbols not subscribing?**  
  **A:** Vendor has set segment symbol limit to 0 for these. Filter them out.
- **Q: Why do I get "No 15s candles"?**  
  **A:** Vendor/websocket must provide these. Otherwise, ensure the pipeline and validator are configured accordingly.
- **Q: Retention policy errors?**  
  **A:** Make sure TimescaleDB is installed, or skip retention checks.
- **Q: Why do I see 'Max Symbol Limit Reached' in logs?**  
  **A:** These are now filtered out automatically, but review your subscription logic if you need more than 70 symbols.

---

## 📬 Contact & Support

- For API/data issues: your market data vendor.
- For pipeline/codebase: open an issue or contact the repo maintainer.

---

## 📑 License

MIT or as specified in this repository.
