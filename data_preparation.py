"""
Data Preparation for Scalping ML/LLM Application

This script prepares the collected and processed NSE BankNifty and options data
for use in scalping strategies and ML/LLM applications. It creates feature sets,
generates training data, and provides interfaces for real-time signal generation.

LIVE DATA (15sec) MODE blocks are present but commented out.
OFFLINE/HISTORICAL (1min) MODE blocks are active.
Switch modes by commenting/uncommenting as needed.
"""

import logging
import time
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import argparse
from dotenv import load_dotenv
import sys

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_preparation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DataPreparation")

# Database connection parameters from .env
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}
for k, v in DB_CONFIG.items():
    if not v:
        logger.error(f"Missing {k} in .env file.")
        sys.exit(1)

class DataPreparation:
    def __init__(self, llm_all_data=False):
        self.db_conn = None
        self.db_cursor = None
        self.llm_all_data = llm_all_data

        # Initialize database connection
        self.connect_to_db()

        # Create output directories
        os.makedirs("ml_data", exist_ok=True)
        os.makedirs("llm_data", exist_ok=True)
        os.makedirs("scalping_signals", exist_ok=True)

    def connect_to_db(self):
        """Establish connection to PostgreSQL/TimescaleDB"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = False
            self.db_cursor = self.db_conn.cursor()
            logger.info("Connected to database successfully")
            return True
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            return False

    def get_first_available_instrument(self):
        """Get the first available instrument symbol and id as fallback."""
        self.db_cursor.execute("SELECT instrument_id, symbol FROM instruments LIMIT 1")
        result = self.db_cursor.fetchone()
        if result:
            return result[0], result[1]
        return None, None

    def get_nifty_instrument(self):
        """Get a NIFTY instrument (prefer NIFTY, then BANKNIFTY) or fallback to any instrument."""
        # Try NIFTY first
        self.db_cursor.execute("SELECT instrument_id, symbol FROM instruments WHERE symbol LIKE '%NIFTY%' ORDER BY CASE WHEN symbol LIKE 'NIFTY%' THEN 0 WHEN symbol LIKE 'BANKNIFTY%' THEN 1 ELSE 2 END LIMIT 1")
        result = self.db_cursor.fetchone()
        if result:
            return result[0], result[1]
        # Fallback to any instrument
        return self.get_first_available_instrument()

    def get_best_instrument_with_data(self):
        """Return the first NIFTY/BANKNIFTY instrument with both candles and indicators, else any instrument with data."""
        # Prefer NIFTY/BANKNIFTY with data
        self.db_cursor.execute('''
            SELECT i.instrument_id, i.symbol
            FROM instruments i
            WHERE i.symbol LIKE '%NIFTY%'
            AND EXISTS (SELECT 1 FROM candle_data_1min c WHERE c.instrument_id = i.instrument_id)
            AND EXISTS (SELECT 1 FROM technical_indicators t WHERE t.instrument_id = i.instrument_id AND t.timeframe = '1min')
            ORDER BY CASE WHEN i.symbol LIKE 'NIFTY%' THEN 0 WHEN i.symbol LIKE 'BANKNIFTY%' THEN 1 ELSE 2 END
            LIMIT 1
        ''')
        result = self.db_cursor.fetchone()
        if result:
            return result[0], result[1]
        # Fallback: any instrument with both candles and indicators
        self.db_cursor.execute('''
            SELECT i.instrument_id, i.symbol
            FROM instruments i
            WHERE EXISTS (SELECT 1 FROM candle_data_1min c WHERE c.instrument_id = i.instrument_id)
            AND EXISTS (SELECT 1 FROM technical_indicators t WHERE t.instrument_id = i.instrument_id AND t.timeframe = '1min')
            LIMIT 1
        ''')
        result = self.db_cursor.fetchone()
        if result:
            return result[0], result[1]
        return None, None

    def prepare_scalping_data(self):
        """Prepare data specifically for scalping strategy"""
        try:
            # --- LIVE DATA (15sec) MODE: Uncomment this block for live candle_data_15sec ---
            '''
            self.db_cursor.execute("""
                CREATE OR REPLACE VIEW scalping_data AS
                SELECT 
                    cd."time",
                    i.symbol,
                    i.instrument_type,
                    i.option_type,
                    i.strike_price,
                    cd.open,
                    cd.high,
                    cd.low,
                    cd.close,
                    cd.volume,
                    cd.open_interest,
                    ti.tvi,
                    ti.obv,
                    ti.rsi,
                    ti.pvi,
                    ti.pvt
                FROM candle_data_15sec cd
                JOIN instruments i ON cd.instrument_id = i.instrument_id
                LEFT JOIN technical_indicators ti ON 
                    cd."time" = ti."time" AND 
                    cd.instrument_id = ti.instrument_id AND 
                    ti.timeframe = '15sec'
                WHERE 
                    EXTRACT(HOUR FROM cd."time") >= 9 AND 
                    EXTRACT(HOUR FROM cd."time") < 11
                ORDER BY cd."time" DESC;
            """)
            '''
            # --- OFFLINE/HISTORICAL (1min) MODE: Use this for now ---
            self.db_cursor.execute("""
                CREATE OR REPLACE VIEW scalping_data AS
                SELECT 
                    cd."time",
                    i.symbol,
                    i.instrument_type,
                    i.option_type,
                    i.strike_price,
                    cd.open,
                    cd.high,
                    cd.low,
                    cd.close,
                    cd.volume,
                    cd.open_interest,
                    ti.tvi,
                    ti.obv,
                    ti.rsi,
                    ti.pvi,
                    ti.pvt
                FROM candle_data_1min cd
                JOIN instruments i ON cd.instrument_id = i.instrument_id
                LEFT JOIN technical_indicators ti ON 
                    cd."time" = ti."time" AND 
                    cd.instrument_id = ti.instrument_id AND 
                    ti.timeframe = '1min'
                ORDER BY cd."time" DESC;
            """)
            # Create a function to get the latest scalping data
            self.db_cursor.execute("""
                CREATE OR REPLACE FUNCTION get_latest_scalping_data(
                    p_limit INTEGER DEFAULT 100
                )
                RETURNS TABLE (
                    "time" TIMESTAMP WITH TIME ZONE,
                    symbol VARCHAR,
                    instrument_type VARCHAR,
                    option_type VARCHAR,
                    strike_price NUMERIC,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume BIGINT,
                    open_interest BIGINT,
                    tvi NUMERIC,
                    obv NUMERIC,
                    rsi NUMERIC,
                    pvi NUMERIC,
                    pvt NUMERIC
                ) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT * FROM scalping_data
                    ORDER BY "time" DESC
                    LIMIT p_limit;
                END;
                $$ LANGUAGE plpgsql;
            """)
            # Create a function to get scalping data for a specific instrument
            self.db_cursor.execute("""
                CREATE OR REPLACE FUNCTION get_instrument_scalping_data(
                    p_symbol VARCHAR,
                    p_limit INTEGER DEFAULT 100
                )
                RETURNS TABLE (
                    "time" TIMESTAMP WITH TIME ZONE,
                    symbol VARCHAR,
                    instrument_type VARCHAR,
                    option_type VARCHAR,
                    strike_price NUMERIC,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume BIGINT,
                    open_interest BIGINT,
                    tvi NUMERIC,
                    obv NUMERIC,
                    rsi NUMERIC,
                    pvi NUMERIC,
                    pvt NUMERIC
                ) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT * FROM scalping_data
                    WHERE symbol = p_symbol
                    ORDER BY "time" DESC
                    LIMIT p_limit;
                END;
                $$ LANGUAGE plpgsql;
            """)
            self.db_conn.commit()
            logger.info("Scalping data preparation completed")

            # Generate sample scalping data for testing
            self.db_cursor.execute("SELECT * FROM scalping_data")
            rows = self.db_cursor.fetchall()
            if rows:
                columns = [
                    '"time"', 'symbol', 'instrument_type', 'option_type', 'strike_price',
                    'open', 'high', 'low', 'close', 'volume', 'open_interest',
                    'tvi', 'obv', 'rsi', 'pvi', 'pvt'
                ]
                df = pd.DataFrame(rows, columns=columns)
                df.to_csv('scalping_signals/sample_scalping_data.csv', index=False)
                logger.info(f"Saved sample scalping data with {len(df)} rows")
            else:
                logger.warning("No scalping data available for sample")
            return True
        except Exception as e:
            logger.error(f"Error preparing scalping data: {str(e)}")
            self.db_conn.rollback()
            return False

    def create_scalping_signal_generator(self):
        """
        Create a flexible, robust scalping signal SQL function with multi-indicator, multi-condition logic.
        Useful for client requests and ML, and works for any timeframe in your pipeline.
        """
        try:
            self.db_cursor.execute("DROP FUNCTION IF EXISTS generate_scalping_signals_flexible(VARCHAR, VARCHAR, INTEGER) CASCADE;")
            self.db_cursor.execute("""
                CREATE OR REPLACE FUNCTION generate_scalping_signals_flexible(
                    p_timeframe VARCHAR DEFAULT '1min',
                    p_symbol VARCHAR DEFAULT NULL,
                    p_limit INTEGER DEFAULT 100
                )
                RETURNS TABLE (
                    "time" TIMESTAMP WITH TIME ZONE,
                    symbol VARCHAR,
                    signal_type VARCHAR,
                    close NUMERIC,
                    rsi NUMERIC,
                    obv NUMERIC,
                    tvi NUMERIC,
                    volume BIGINT,
                    ma5 NUMERIC,
                    ma20 NUMERIC,
                    avg_vol20 NUMERIC
                ) AS $$
                DECLARE
                    table_name TEXT;
                    sql TEXT;
                BEGIN
                    table_name := 'candle_data_' || lower(p_timeframe);

                    sql := '
                        SELECT
                            cd."time",
                            i.symbol,
                            CASE
                                WHEN ti.rsi < 30
                                  AND ti.obv > LAG(ti.obv) OVER (ORDER BY cd."time")
                                  AND cd.close > AVG(cd.close) OVER (ORDER BY cd."time" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                                  AND cd.volume > 1.5 * AVG(cd.volume) OVER (ORDER BY cd."time" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                                  THEN ''buy''::varchar
                                WHEN ti.rsi > 70
                                  AND ti.obv < LAG(ti.obv) OVER (ORDER BY cd."time")
                                  AND cd.close < AVG(cd.close) OVER (ORDER BY cd."time" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                                  AND cd.volume > 1.5 * AVG(cd.volume) OVER (ORDER BY cd."time" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                                  THEN ''sell''::varchar
                                WHEN cd.close > AVG(cd.close) OVER (ORDER BY cd."time" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                                  AND ti.rsi > LAG(ti.rsi) OVER (ORDER BY cd."time")
                                  THEN ''trend_up''::varchar
                                WHEN cd.close < AVG(cd.close) OVER (ORDER BY cd."time" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                                  AND ti.rsi < LAG(ti.rsi) OVER (ORDER BY cd."time")
                                  THEN ''trend_down''::varchar
                                ELSE ''hold''::varchar
                            END AS signal_type,
                            cd.close,
                            ti.rsi,
                            ti.obv,
                            ti.tvi,
                            cd.volume,
                            AVG(cd.close) OVER (ORDER BY cd."time" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5,
                            AVG(cd.close) OVER (ORDER BY cd."time" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20,
                            AVG(cd.volume) OVER (ORDER BY cd."time" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_vol20
                        FROM ' || table_name || ' cd
                        JOIN instruments i ON cd.instrument_id = i.instrument_id
                        LEFT JOIN technical_indicators ti ON
                            cd."time" = ti."time" AND
                            cd.instrument_id = ti.instrument_id AND
                            ti.timeframe = $1
                    ';

                    IF p_symbol IS NOT NULL THEN
                        sql := sql || ' WHERE i.symbol = $2 ';
                        sql := sql || ' ORDER BY cd."time" DESC LIMIT $3 ';
                        RETURN QUERY EXECUTE sql USING p_timeframe, p_symbol, p_limit;
                    ELSE
                        sql := sql || ' ORDER BY cd."time" DESC LIMIT $2 ';
                        RETURN QUERY EXECUTE sql USING p_timeframe, p_limit;
                    END IF;
                END;
                $$ LANGUAGE plpgsql;
            """)
            self.db_conn.commit()
            logger.info("Flexible scalping signal generator (generate_scalping_signals_flexible) created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating flexible scalping signal generator: {str(e)}")
            self.db_conn.rollback()
            return False

    def create_feature_engineering_functions(self):
        """Create SQL functions for feature engineering"""
        try:
            self.db_cursor.execute("""
                CREATE OR REPLACE FUNCTION calculate_moving_average(
                    p_instrument_id INTEGER,
                    p_timeframe VARCHAR,
                    p_window INTEGER
                )
                RETURNS TABLE (
                    "time" TIMESTAMP WITH TIME ZONE,
                    ma NUMERIC
                ) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT 
                        "time",
                        AVG(close) OVER (
                            PARTITION BY instrument_id 
                            ORDER BY "time" 
                            ROWS BETWEEN p_window-1 PRECEDING AND CURRENT ROW
                        ) AS ma
                    FROM candle_data_1min
                    WHERE instrument_id = p_instrument_id
                    ORDER BY "time" DESC;
                END;
                $$ LANGUAGE plpgsql;
            """)
            self.db_conn.commit()
            logger.info("Feature engineering functions created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating feature engineering functions: {str(e)}")
            self.db_conn.rollback()
            return False

    def prepare_ml_training_data(self):
        """Prepare data for machine learning model training"""
        try:
            instrument_id, symbol = self.get_best_instrument_with_data()
            if not instrument_id:
                logger.error("No instrument with both candles and indicators found in database.")
                return False
            logger.info(f"Using symbol: {symbol}")

            self.db_cursor.execute("SELECT COUNT(*) FROM technical_indicators WHERE instrument_id = %s AND timeframe='1min'", (instrument_id,))
            indicator_count = self.db_cursor.fetchone()[0]
            self.db_cursor.execute("SELECT COUNT(*) FROM candle_data_1min WHERE instrument_id = %s", (instrument_id,))
            candle_count = self.db_cursor.fetchone()[0]
            logger.info(f"Found {indicator_count} indicators and {candle_count} 1min candles for {symbol}")

            self.db_cursor.execute("""
                SELECT 
                    cd."time",
                    cd.open,
                    cd.high,
                    cd.low,
                    cd.close,
                    cd.volume,
                    cd.open_interest,
                    ti.tvi,
                    ti.obv,
                    ti.rsi,
                    ti.pvi,
                    ti.pvt,
                    LEAD(cd.close, 4) OVER (ORDER BY cd."time") AS future_price
                FROM candle_data_1min cd
                JOIN technical_indicators ti ON 
                    cd."time" = ti."time" AND 
                    cd.instrument_id = ti.instrument_id AND 
                    ti.timeframe = '1min'
                WHERE 
                    cd.instrument_id = %s
                ORDER BY cd."time" ASC
            """, (instrument_id,))

            rows = self.db_cursor.fetchall()
            if not rows:
                logger.warning(f"No data available for ML training for instrument_id={instrument_id} ({symbol})")
                return False

            columns = [
                '"time"', 'open', 'high', 'low', 'close', 'volume', 'open_interest',
                'tvi', 'obv', 'rsi', 'pvi', 'pvt', 'future_price'
            ]
            df = pd.DataFrame(rows, columns=columns)
            df = df.dropna(subset=['future_price'])
            if df.empty:
                logger.warning(f"All joined data dropped due to missing future_price for instrument_id={instrument_id} ({symbol})")
                return False

            df['target'] = (df['future_price'] > df['close']).astype(int)
            df['price_change'] = df['close'].pct_change()
            df['volume_change'] = df['volume'].pct_change()
            df['oi_change'] = df['open_interest'].pct_change()
            df['ma5'] = df['close'].rolling(window=5).mean()
            df['ma20'] = df['close'].rolling(window=20).mean()
            df['rsi_change'] = df['rsi'].diff()
            df = df.dropna()
            if df.empty:
                logger.warning(f"All data dropped after feature engineering for instrument_id={instrument_id} ({symbol})")
                return False

            df.to_csv('ml_data/full_dataset.csv', index=False)
            features = [
                'open', 'high', 'low', 'close', 'volume', 'open_interest',
                'tvi', 'obv', 'rsi', 'pvi', 'pvt',
                'price_change', 'volume_change', 'oi_change',
                'ma5', 'ma20', 'rsi_change'
            ]
            X = df[features]
            y = df['target']

            X = X.replace([np.inf, -np.inf], np.nan)
            X = X.dropna()
            X = X.clip(lower=-1e10, upper=1e10)
            y = y.loc[X.index]

            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            with open('ml_data/scaler.pkl', 'wb') as f:
                pickle.dump(scaler, f)
            X_train, X_test, y_train, y_test = train_test_split(
                X_scaled, y, test_size=0.2, random_state=42
            )
            np.save('ml_data/X_train.npy', X_train)
            np.save('ml_data/X_test.npy', X_test)
            np.save('ml_data/y_train.npy', y_train)
            np.save('ml_data/y_test.npy', y_test)
            with open('ml_data/feature_names.txt', 'w') as f:
                f.write('\n'.join(features))
            logger.info(f"ML training data prepared successfully with {len(df)} samples for symbol {symbol}")

            plt.figure(figsize=(12, 8))
            ax1 = plt.subplot(3, 1, 1)
            ax1.plot(df['"time"'], df['close'], label='Close Price')
            ax1.plot(df['"time"'], df['ma5'], label='MA5', alpha=0.7)
            ax1.plot(df['"time"'], df['ma20'], label='MA20', alpha=0.7)
            ax1.set_title(f'{symbol} Price and Indicators')
            ax1.set_ylabel('Price')
            ax1.legend()
            ax2 = plt.subplot(3, 1, 2, sharex=ax1)
            ax2.plot(df['"time"'], df['rsi'], label='RSI')
            ax2.axhline(y=70, color='r', linestyle='-', alpha=0.3)
            ax2.axhline(y=30, color='g', linestyle='-', alpha=0.3)
            ax2.set_ylabel('RSI')
            ax2.set_ylim(0, 100)
            ax2.legend()
            ax3 = plt.subplot(3, 1, 3, sharex=ax1)
            buy_signals = df[df['target'] == 1]
            sell_signals = df[df['target'] == 0]
            ax3.scatter(buy_signals['"time"'], buy_signals['close'], color='green', label='Buy Signal', marker='^', alpha=0.7)
            ax3.scatter(sell_signals['"time"'], sell_signals['close'], color='red', label='Sell Signal', marker='v', alpha=0.7)
            ax3.set_ylabel('Signals')
            ax3.legend()
            plt.tight_layout()
            plt.savefig('ml_data/training_data_visualization.png')
            plt.close()
            return True
        except Exception as e:
            logger.error(f"Error preparing ML training data: {str(e)}")
            return False

    def prepare_llm_data(self):
        try:
            if self.llm_all_data:
                latest_prices_where = ""
                latest_indicators_where = "WHERE ti.timeframe = '1min'"
                logger.info("Preparing LLM data using ALL available historical data (no time filter).")
            else:
                latest_prices_where = "WHERE cd.\"time\" >= NOW() - INTERVAL '30 minutes'"
                latest_indicators_where = "WHERE ti.timeframe = '1min' AND ti.\"time\" >= NOW() - INTERVAL '30 minutes'"
                logger.info("Preparing LLM data using ONLY the last 30 minutes of data (default behavior).")

            query = f"""
                CREATE OR REPLACE VIEW market_summary AS
                WITH latest_prices AS (
                    SELECT 
                        i.symbol,
                        i.instrument_type,
                        i.option_type,
                        i.strike_price,
                        cd.close AS latest_price,
                        cd."time" AS latest_time,
                        LAG(cd.close) OVER (PARTITION BY i.instrument_id ORDER BY cd."time" DESC) AS prev_price,
                        cd.volume,
                        cd.open_interest
                    FROM instruments i
                    JOIN candle_data_1min cd ON i.instrument_id = cd.instrument_id
                    {latest_prices_where}
                    ORDER BY i.symbol, cd."time" DESC
                ),
                latest_indicators AS (
                    SELECT 
                        i.symbol,
                        ti.rsi,
                        ti.tvi,
                        ti.obv,
                        ti."time"
                    FROM instruments i
                    JOIN technical_indicators ti ON i.instrument_id = ti.instrument_id
                    {latest_indicators_where}
                    ORDER BY i.symbol, ti."time" DESC
                )
                SELECT 
                    lp.symbol,
                    lp.instrument_type,
                    lp.option_type,
                    lp.strike_price,
                    lp.latest_price,
                    lp.latest_time,
                    ROUND((lp.latest_price - lp.prev_price) / NULLIF(lp.prev_price, 0) * 100, 2) AS price_change_pct,
                    lp.volume,
                    lp.open_interest,
                    li.rsi,
                    li.tvi,
                    li.obv,
                    CASE
                        WHEN li.rsi > 70 THEN 'Overbought'
                        WHEN li.rsi < 30 THEN 'Oversold'
                        ELSE 'Neutral'
                    END AS rsi_signal
                FROM latest_prices lp
                LEFT JOIN latest_indicators li ON lp.symbol = li.symbol AND lp.latest_time = li."time"
                WHERE lp.prev_price IS NOT NULL
                ORDER BY lp.symbol;
            """
            self.db_cursor.execute(query)
            self.db_conn.commit()

            self.db_cursor.execute("SELECT * FROM market_summary")
            rows = self.db_cursor.fetchall()
            if rows:
                columns = [desc[0] for desc in self.db_cursor.description]
                market_data = []
                for row in rows:
                    market_data.append(dict(zip(columns, row)))
                df = pd.DataFrame(market_data)
                # Robust handling: drop/fill NaN/Inf
                df = df.replace([np.inf, -np.inf], np.nan).dropna()
                df.to_csv('llm_data/market_data.csv', index=False)
                df.to_json('llm_data/market_data.json', orient='records', date_format='iso')
                logger.info(f"LLM data prepared successfully with {len(df)} records")
            else:
                logger.warning("No market data available for LLM preparation")
            return True
        except Exception as e:
            logger.error(f"Error preparing LLM data: {str(e)}")
            self.db_conn.rollback()
            return False

    def run(self):
        try:
            logger.info("Starting data preparation for ML/LLM applications")
            if self.prepare_scalping_data():
                logger.info("Scalping data preparation completed successfully")
            else:
                logger.error("Scalping data preparation failed")
            if self.create_feature_engineering_functions():
                logger.info("Feature engineering functions created successfully")
            else:
                logger.error("Feature engineering functions creation failed")
            if self.create_scalping_signal_generator():
                logger.info("Scalping signal generator created successfully")
            else:
                logger.error("Scalping signal generator creation failed")
            if self.prepare_ml_training_data():
                logger.info("ML training data preparation completed successfully")
            else:
                logger.error("ML training data preparation failed")
            if self.prepare_llm_data():
                logger.info("LLM data preparation completed successfully")
            else:
                logger.error("LLM data preparation failed")
            logger.info("Data preparation completed")
            return True
        except Exception as e:
            logger.error(f"Error in data preparation: {str(e)}")
            return False
        finally:
            if self.db_conn:
                self.db_conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Preparation for Scalping ML/LLM Application")
    parser.add_argument('--offline', action='store_true', help='Run in offline mode with historical data file')
    parser.add_argument('--input', type=str, help='Path to historical data file (CSV/JSON)')
    parser.add_argument('--llm-all-data', action='store_true', help='Use ALL available data for LLM/market summary instead of just last 30 minutes')
    args = parser.parse_args()

    if args.offline and args.input:
        logger.info(f"Running in offline mode with file: {args.input}")
        try:
            if args.input.endswith('.csv'):
                df = pd.read_csv(args.input)
                logger.info(f"Loaded {len(df)} rows from CSV")
            else:
                logger.warning("Only CSV input supported for offline mode in this script")
        except Exception as e:
            logger.error(f"Error loading offline input: {str(e)}")
        dp = DataPreparation(llm_all_data=args.llm_all_data)
        dp.run()
    else:
        dp = DataPreparation(llm_all_data=args.llm_all_data)
        dp.run()