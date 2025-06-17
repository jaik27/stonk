import os
import time
import glob
import logging
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from schema_design import import_historical_csv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

LOGIN_ID = os.getenv("NSE_LOGIN_ID")
PRODUCT = os.getenv("NSE_PRODUCT")
API_KEY = os.getenv("NSE_API_KEY")
AUTH_ENDPOINT = os.getenv("NSE_AUTH_ENDPOINT")

def get_auth_token():
    url = f"{AUTH_ENDPOINT}?loginid={LOGIN_ID}&product={PRODUCT}&apikey={API_KEY}"
    try:
        res = requests.get(url)
        if res.status_code == 200:
            return res.json()['AccessToken']
        else:
            raise Exception(f"Failed to get token: {res.text}")
    except Exception as e:
        logging.error(f"Error fetching token: {e}")
        return None

def convert_date_format(date_str):
    for fmt in ["%d%b%Y", "%d-%b-%Y", "%d-%b-%y", "%d%b%y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%d%b%Y").upper()
        except Exception:
            continue
    return date_str.upper()

def get_historical_data(access_token, startdate, enddate, exch, inst, symbol, expiry=None, strike=None, optiontype=None, delay=0.6):
    url = (
        f"http://qbase1.vbiz.in/directrt/gethistorical?loginid={LOGIN_ID}&product={PRODUCT}&accesstoken={access_token}"
        f"&startdate={startdate}&enddate={enddate}&exch={exch}&inst={inst}&symbol={symbol}"
    )
    if expiry: url += f"&expiry={expiry}"
    if strike: url += f"&strike={strike}"
    if optiontype: url += f"&optiontype={optiontype}"
    if delay > 0: time.sleep(delay)
    try:
        res = requests.get(url, timeout=30)
        if res.status_code == 200:
            from io import StringIO
            return pd.read_csv(StringIO(res.text))
        else:
            logging.warning(f"Non-200 response: {res.status_code} - {res.text[:100]}")
            return None
    except Exception as e:
        logging.error(f"Network error: {e}")
        return None

def combine_csvs_to_master(batch_dir, output_name="combined_master.csv"):
    csv_files = glob.glob(os.path.join(batch_dir, "*.csv"))
    dfs = []
    for csvf in csv_files:
        if os.path.basename(csvf) == output_name:
            continue
        try:
            df = pd.read_csv(csvf)
            df['__source_file'] = os.path.basename(csvf)
            dfs.append(df)
        except Exception as e:
            logging.warning(f"Error reading {csvf}: {e}")
    if not dfs:
        logging.warning("No CSVs to combine.")
        return None
    combined = pd.concat(dfs, ignore_index=True)
    master_path = os.path.join(batch_dir, output_name)
    combined.to_csv(master_path, index=False)
    logging.info(f"Combined file written to: {master_path}")
    return master_path

def import_all_csvs(batch_dir):
    csv_files = glob.glob(os.path.join(batch_dir, "*.csv"))
    imported = 0
    failed = 0
    for csvf in csv_files:
        if os.path.basename(csvf).startswith("combined_master"):
            continue
        try:
            import_historical_csv(csvf, candle_type="1min")
            imported += 1
        except Exception as e:
            logging.error(f"Import error for {csvf}: {e}")
            failed += 1
    logging.info(f"Imported {imported} CSVs, {failed} failed.")
    return imported, failed

def process_nifty_input_file(
    input_file="Nifty_Input.csv", 
    max_rows=None, 
    api_delay=0.6, 
    base_output_dir="historical_data"
):
    batch_dir = os.path.join(
        base_output_dir,
        f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    )
    os.makedirs(batch_dir, exist_ok=True)
    df = pd.read_csv(input_file)
    if max_rows:
        df = df.head(max_rows)
    logging.info(f"Processing {len(df)} rows from {input_file}")

    access_token = get_auth_token()
    if not access_token:
        logging.error("Authentication failed. Exiting.")
        return

    total_files = 0
    for _, row in df.iterrows():
        symbol = row['Symbol']
        start_date = convert_date_format(str(row['StartDate']))
        end_date = convert_date_format(str(row['EndDate']))
        expiry = convert_date_format(str(row['Expiry']))
        low_strike = int(row['Low'])
        high_strike = int(row['High'])
        strikes = range(low_strike, high_strike + 1, 50)
        option_types = ['CE', 'PE']
        for strike in strikes:
            for option_type in option_types:
                filename = f"{symbol}{expiry}{strike}{option_type}.csv"
                out_path = os.path.join(batch_dir, filename)
                data = get_historical_data(
                    access_token, start_date, end_date, "NSE", "OPTIDX",
                    symbol, expiry, str(strike), option_type, api_delay
                )
                if data is not None and not data.empty:
                    data.to_csv(out_path, index=False)
                    total_files += 1
                    logging.info(f"Saved {out_path} ({len(data)} rows)")
                else:
                    logging.info(f"No data for {symbol} {expiry} {strike} {option_type}")

    logging.info(f"Download complete. {total_files} CSV files generated in {batch_dir}")
    master_csv_path = combine_csvs_to_master(batch_dir)
    logging.info("Starting DB import...")
    imported, failed = import_all_csvs(batch_dir)
    logging.info(f"ALL DONE. {imported} files imported, {failed} failed. Master CSV: {master_csv_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Batch historical downloader for Nifty option chains.")
    parser.add_argument("--input", default="Nifty_Input.csv", help="CSV input file")
    parser.add_argument("--max-rows", type=int, default=None, help="Max rows to process")
    parser.add_argument("--delay", type=float, default=0.6, help="API delay in seconds")
    parser.add_argument("--output-dir", default="historical_data", help="Base output directory for CSV files")
    args = parser.parse_args()
    process_nifty_input_file(args.input, args.max_rows, args.delay, args.output_dir)