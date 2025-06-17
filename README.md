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

2. **Data Collection** (`data_collector.py`)
   - Downloads tickers.
   - Connects to the vendor websocket/API.
   - Subscribes to all relevant tickers.
   - Writes live tick/candle data to the database.
   - Processes tick data into 1-minute and 15-second candles.
   - Stores data in TimescaleDB with 7-day retention policy (if TimescaleDB is enabled).

3. **Indicator Calculation** (`indicator_calculator.py`)
   - Loads recent candle data.
   - Computes technical indicators:
     - On-Balance Volume (OBV)
     - Relative Strength Index (RSI)
     - Trend Volume Index (TVI)
     - Positive Volume Index (PVI)
     - Price Volume Trend (PVT)
   - Inserts results into the `technical_indicators` table.

4. **Pipeline Validation** (`pipeline_validator.py`)
   - Ensures all tables and views exist.
   - Validates that both recent and historical data are present and correct.
   - Checks indicator completeness and data retention.
   - Verifies timezones, technical indicator coverage, segment subscription, and more.

5. **Data Preparation** (`data_preparation.py`)
   - Prepares and exports data for ML/trading.
   - Supports parameterized ML dataset generation per chosen contract ("slice").

**No step is optional.  
Do not attempt to run any component directly; always use `app.py`.**

---

## 🗂️ File Structure

```
.
├── app.py                     # Main orchestrator, run this!
├── app_gui.py                 # Interactive Streamlit GUI for data/ML exploration and on-demand ML training
├── schema_design.py           # Critical: schema + historical data loader (always runs first)
├── data_collector.py
├── indicator_calculator.py
├── pipeline_validator.py
├── data_preparation.py
├── requirements.txt
├── .env
├── historical_data/           # Auto-generated: stores imported/downloaded historical candle CSVs
├── ml_data/                   # Auto-generated: stores ML datasets, scalers, train/test splits, plots
├── llm_data/                  # Auto-generated: LLM/market summary data for chat/analytics
├── scalping_signals/          # Auto-generated: sample scalping data, CSVs
├── validation_reports/        # Auto-generated: pipeline and data validation reports
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
pip install python-dotenv
pip install streamlit
pip install psycopg2-binary
pip install scikit-learn
pip install pandas
pip install matplotlib

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
- 
**Do NOT run component scripts directly. Only use `app.py`.**

---

## 📝 Important Notes

- **schema_design.py runs FIRST and is critical.**
- **Historical data import is always included.**
- **All tables/views:**  
  `instruments`, `candle_data_1min`, `candle_data_15sec`, `technical_indicators`, `trading_signals`, `first_two_hours_data`
- **TimescaleDB retention:**  
  If you see errors about missing `_timescaledb_config.bgw_policy_drop_chunks`, check TimescaleDB install or skip retention validation if not using it.
- **Timezone:**  
  Always store/process all datetimes as UTC and timezone-aware.
- **MCX/OPTCOM subscription errors:**  
  If the vendor's segment limit is 0, filter these out in `filter_relevant_tickers()` in `data_collector.py`.
- **15s candle data:**  
  If the vendor doesn't supply 15s candles, edit the pipeline and validation to skip or ensure this data is present.
- **Technical indicators:**  
  Ensure the indicator calculator is running and writing to DB.
- **Start everything using `app.py`.**
- **GUI (`app_gui.py`):**
  - Explore contracts, download candles, visualize signals, and trigger ML training for any contract slice.
  - After ML training, you'll see a summary, preview, charts, and explanations for the generated ML dataset.

---

## 🧑‍💻 Running as a Service on Windows

- Use Windows Task Scheduler or NSSM to run `app.py` as a background service.
- Always activate your venv in the task's startup command.

---

## ❓ FAQ

- **Q: How do I start the whole system?**  
  **A:** Run `python app.py` in your activated virtual environment. This always initializes the schema and pulls historical data before any other step.
- **Q: How do I use the GUI for analysis and ML training?**  
  **A:** Run `streamlit run app_gui.py` after starting the backend; use the sidebar to select contracts and the main panel to explore data and trigger per-contract ML training.
- **Q: Why are MCX symbols not subscribing?**  
  **A:** Vendor has set segment symbol limit to 0 for these. Filter them out.
- **Q: Why do I get "No 15s candles"?**  
  **A:** Vendor/websocket must provide these. Otherwise, ensure the pipeline and validator are configured accordingly.
- **Q: Retention policy errors?**  
  **A:** Make sure TimescaleDB is installed, or skip retention checks.
- **Q: Why do I get "Permission denied" on ML output files?**  
  **A:** Ensure `ml_data/full_dataset.csv` isn't open in Excel or marked read-only; close all viewers and rerun.

---

## 📬 Contact & Support

- For API/data issues: your market data vendor.
- For pipeline/codebase: open an issue or contact the repo maintainer.

---

## 📑 License

MIT or as specified in this repository.

---

## 📂 Example Directory Tree (after running `app.py`)

```
C:\Users\300353635\Desktop\stock-trading-app
│   .env
│   app.py
│   app_gui.py
│   data_collector.py
│   data_collector.log
│   data_preparation.py
│   data_preparation.log
│   historical_data.csv
│   historical_downloader.py
│   indicator_calculator.py
│   indicator_calculator.log
│   Nifty_Input.csv
│   pipeline_validator.py
│   pipeline_validator.log
│   README.md
│   requirements.txt
│   schema_design.py
│   Socketcluster.py
│   tickers.txt
│
├── __pycache__/
├── historical_data/
├── llm_data/
├── ml_data/
├── scalping_signals/
├── validation_reports/
├── venv/
```

- **Folders like `ml_data/`, `scalping_signals/`, `llm_data/`, and `validation_reports/` are auto-generated and will contain CSVs and plots after running the pipeline and GUI.**