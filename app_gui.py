import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date
from dotenv import load_dotenv
load_dotenv()
import os
import matplotlib.pyplot as plt

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

@st.cache_data
def load_instruments():
    with get_connection() as conn:
        df = pd.read_sql("SELECT * FROM instruments", conn)
    return df

def load_candles(instrument_id, start, end, timeframe="1min"):
    table = f"candle_data_{timeframe}"
    instrument_id = int(instrument_id)  # Ensure Python int for SQL
    with get_connection() as conn:
        df = pd.read_sql(
            f"SELECT * FROM {table} WHERE instrument_id=%s AND time BETWEEN %s AND %s ORDER BY time",
            conn, params=(instrument_id, start, end)
        )
    return df

def load_signals(symbol, timeframe, limit=200):
    with get_connection() as conn:
        df = pd.read_sql(
            "SELECT * FROM generate_scalping_signals_flexible(%s, %s, %s)",
            conn, params=(timeframe, symbol, limit)
        )
    return df

# --- SIDEBAR: Instrument Picker ---
st.sidebar.header("Instrument Picker")
inst_df = load_instruments()

# Prefer NIFTY/BANKNIFTY first, then everything else
all_underlyings = inst_df['symbol'].str.extract(r'([A-Z]+)')[0].dropna().unique()
priority = ["NIFTY", "BANKNIFTY"]
ordered_underlyings = [u for u in priority if u in all_underlyings] + [u for u in all_underlyings if u not in priority]
underlying = st.sidebar.selectbox("Underlying (e.g. NIFTY, BANKNIFTY)", ordered_underlyings, index=0)

expiry_options = sorted(inst_df[inst_df['symbol'].str.startswith(underlying)]['expiry_date'].dropna().unique())
expiry = st.sidebar.selectbox("Expiry Date", expiry_options, format_func=lambda x: x.strftime("%d-%b-%Y") if pd.notnull(x) else "Unknown")

strike_options = sorted(inst_df[
    (inst_df['symbol'].str.startswith(underlying)) &
    (inst_df['expiry_date']==expiry)
]['strike_price'].dropna().unique())
strike = st.sidebar.selectbox("Strike Price", strike_options)

option_type = st.sidebar.selectbox("Option Type", ['CE', 'PE'])

filtered = inst_df[
    (inst_df['symbol'].str.startswith(underlying)) &
    (inst_df['expiry_date']==expiry) &
    (inst_df['strike_price']==strike) &
    (inst_df['option_type']==option_type)
]
if filtered.empty:
    st.warning("No such option contract found in DB!")
    st.stop()
instrument_id = filtered.iloc[0]['instrument_id']
symbol = filtered.iloc[0]['symbol']

st.title("NSE Options Data Explorer")
st.markdown(f"""
**Selected Contract:**  
- **Symbol:** `{symbol}`  
- **Expiry:** `{expiry.strftime('%d-%b-%Y')}`  
- **Strike:** `{strike}`  
- **Type:** `{option_type}`
""")

# --- Historical Data Section ---
mindate = filtered.iloc[0].get('expiry_date', date(2020,1,1))
start_date = st.date_input("Start Date", value=mindate)
end_date = st.date_input("End Date", value=mindate if mindate > date.today() else date.today())
timeframe = st.radio("Candle Timeframe", ["1min", "15sec"])

df = load_candles(instrument_id, start_date, end_date, timeframe)
if df.empty:
    st.warning("No candle data for this contract and range.")
else:
    st.subheader("Candlestick Data")
    st.dataframe(df, use_container_width=True)
    st.line_chart(df.set_index('time')['close'], use_container_width=True)
    st.line_chart(df.set_index('time')['volume'], use_container_width=True)
    st.download_button("Download Candle CSV", df.to_csv(index=False), file_name=f"{symbol}_{timeframe}.csv")

# --- Scalping Signals Section with Plot ---
st.subheader("Scalping Signals (Multi-indicator)")
if st.button("Show Scalping Signals"):
    sig_df = load_signals(symbol, timeframe)
    if not sig_df.empty:
        # Plot signal markers on close price
        fig, ax = plt.subplots(figsize=(12, 5))
        x = pd.to_datetime(sig_df['time']) if 'time' in sig_df.columns else sig_df.index
        ax.plot(x, sig_df['close'], label='Close', color='blue')
        if 'signal_type' in sig_df.columns:
            buy = sig_df[sig_df['signal_type'] == 'buy']
            sell = sig_df[sig_df['signal_type'] == 'sell']
            trend_up = sig_df[sig_df['signal_type'] == 'trend_up']
            trend_down = sig_df[sig_df['signal_type'] == 'trend_down']
            hold = sig_df[sig_df['signal_type'] == 'hold']
            if not buy.empty:
                ax.scatter(pd.to_datetime(buy['time']), buy['close'], marker='^', color='green', label='Buy', alpha=0.9)
            if not sell.empty:
                ax.scatter(pd.to_datetime(sell['time']), sell['close'], marker='v', color='red', label='Sell', alpha=0.9)
            if not trend_up.empty:
                ax.scatter(pd.to_datetime(trend_up['time']), trend_up['close'], marker='o', color='orange', label='Trend Up', alpha=0.7)
            if not trend_down.empty:
                ax.scatter(pd.to_datetime(trend_down['time']), trend_down['close'], marker='o', color='purple', label='Trend Down', alpha=0.7)
        ax.set_title("Scalping Signals on Price")
        ax.set_ylabel("Price")
        ax.set_xlabel("Time")
        ax.legend()
        st.pyplot(fig)

        st.dataframe(sig_df, use_container_width=True)
        st.download_button("Download Signals CSV", sig_df.to_csv(index=False), file_name=f"{symbol}_{timeframe}_signals.csv")
        st.markdown("""
**Signal Types:**
- `buy`: RSI < 30 (oversold), OBV increasing, close > MA5 (momentum up), volume spike.
- `sell`: RSI > 70 (overbought), OBV decreasing, close < MA5 (momentum down), volume spike.
- `trend_up`: Close > MA20 and RSI rising.
- `trend_down`: Close < MA20 and RSI falling.
- `hold`: None of the above (neutral/wait).
        """)
    else:
        st.info("No signals found for this selection.")

# --- ML Buy/Sell Signals Section with Plot ---
st.subheader("ML Buy/Sell Signals (from ml_data/full_dataset.csv)")
ml_path = os.path.join("ml_data", "full_dataset.csv")
if os.path.exists(ml_path):
    ml_df = pd.read_csv(ml_path)
    # Filter by symbol if present (assumes symbol column exists)
    if 'symbol' in ml_df.columns:
        ml_df = ml_df[ml_df['symbol'] == symbol]
    if 'expiry_date' in ml_df.columns:
        ml_df = ml_df[ml_df['expiry_date'] == expiry]
    if 'strike_price' in ml_df.columns:
        ml_df = ml_df[ml_df['strike_price'] == strike]
    if 'option_type' in ml_df.columns:
        ml_df = ml_df[ml_df['option_type'] == option_type]

    if not ml_df.empty:
        fig, ax = plt.subplots(figsize=(12, 5))
        # X-axis: time column or index
        if '"time"' in ml_df.columns:
            x = pd.to_datetime(ml_df['"time"'])
        elif 'time' in ml_df.columns:
            x = pd.to_datetime(ml_df['time'])
        else:
            x = ml_df.index
        ax.plot(x, ml_df['close'], label='Close', color='blue')
        if 'target' in ml_df.columns:
            buys = ml_df[ml_df['target'] == 1]
            sells = ml_df[ml_df['target'] == 0]
            if not buys.empty:
                if '"time"' in buys.columns:
                    x_buys = pd.to_datetime(buys['"time"'])
                elif 'time' in buys.columns:
                    x_buys = pd.to_datetime(buys['time'])
                else:
                    x_buys = buys.index
                ax.scatter(x_buys, buys['close'], marker='^', color='green', label='Buy', alpha=0.9)
            if not sells.empty:
                if '"time"' in sells.columns:
                    x_sells = pd.to_datetime(sells['"time"'])
                elif 'time' in sells.columns:
                    x_sells = pd.to_datetime(sells['time'])
                else:
                    x_sells = sells.index
                ax.scatter(x_sells, sells['close'], marker='v', color='red', label='Sell', alpha=0.9)
        ax.set_title("ML Buy/Sell Signals on Price")
        ax.set_ylabel("Price")
        ax.set_xlabel("Time")
        ax.legend()
        st.pyplot(fig)
        st.download_button("Download ML Buy/Sell Signals CSV", ml_df.to_csv(index=False), file_name=f"{symbol}_ml_signals.csv")
    else:
        st.info("No ML signals found for this contract in ml_data/full_dataset.csv.")
else:
    st.info("ML data file not found (ml_data/full_dataset.csv).")

# --- ML Training Button ---
if st.button("Run ML Training on this slice"):
    st.info("ML pipeline trigger placeholder. You can call your backend ML script with these parameters.")

# --- Download Section for Combined/ML/Scalping Data ---
st.subheader("Download Data Files (Raw/ML Sample)")
col1, col2, col3 = st.columns(3)
with col1:
    cmaster_path = os.path.join("historical_data", "combined_master.csv")
    if os.path.exists(cmaster_path):
        with open(cmaster_path, "rb") as f:
            st.download_button("Download Raw Combined Master CSV", data=f, file_name="combined_master.csv", mime="text/csv")
with col2:
    ml_path = os.path.join("ml_data", "full_dataset.csv")
    if os.path.exists(ml_path):
        with open(ml_path, "rb") as f:
            st.download_button("Download ML Full Dataset CSV", data=f, file_name="full_dataset.csv", mime="text/csv")
with col3:
    sample_path = os.path.join("scalping_signals", "sample_scalping_data.csv")
    if os.path.exists(sample_path):
        with open(sample_path, "rb") as f:
            st.download_button("Download Sample Scalping Data CSV", data=f, file_name="sample_scalping_data.csv", mime="text/csv")

with st.expander("❓ How to use this tool / Signal logic"):
    st.markdown("""
**How to use:**
- **Pick Underlying, Expiry, Strike, and Option Type** to select the exact contract you want.
- **Pick date range and candle timeframe** (1min or 15sec).
- **See/download raw candles** and **scalping signals** instantly.
- **See/download ML buy/sell signals** from the ML output file, including a plot.
- Use "Run ML Training" to trigger model training for the chosen slice (requires backend hook).
- **Download Raw/ML/Sample Data** from the section above if you want to work with the CSVs directly.

**Signal Logic (for ML and trading):**
- **buy**: RSI < 30 (oversold), OBV rising, close > MA5, volume > 1.5 × 20-period average
- **sell**: RSI > 70 (overbought), OBV falling, close < MA5, volume > 1.5 × 20-period average
- **trend_up**: Close > MA20 & RSI rising
- **trend_down**: Close < MA20 & RSI falling
- **hold**: none of the above

**Columns:**
- `close`: Last price
- `rsi`, `obv`, `tvi`: Indicators
- `ma5`, `ma20`: 5/20-period moving averages
- `avg_vol20`: 20-period average volume

You can use these as features for ML, and `signal_type` as a label.
    """)

with st.expander("🗒️ Client Request Coverage"):
    st.markdown("""
- **Historical data:** Select any symbol/expiry/strike/type, date range, and view candles.
- **Weekly symbol/ticker changes:** GUI auto-loads all available contracts from DB.
- **On-demand signals:** Select any contract and instantly view/download rich signals for any expiry/strike/type.
- **ML Training:** Use the "Run ML Training" button to trigger backend ML training for the selected data slice.
- **Download Section:** Download raw historical (combined_master.csv), ML full dataset, and sample scalping data CSVs.
    """)