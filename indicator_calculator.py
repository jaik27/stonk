import logging
import time
import threading
import queue
import psycopg2
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("indicator_calculator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("IndicatorCalculator")

DB_CONFIG = {
    "dbname": "nse_data",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

try:
    from data_collector import data_queue as processed_queue
except ImportError:
    processed_queue = queue.Queue()

class IndicatorCalculator:
    def __init__(self, mode="batch", live_max_items=None, live_timeout=None):
        self.db_conn = None
        self.db_cursor = None
        self.running = False
        self.data_thread = None
        self.mode = mode
        self.data_cache_1min = {}
        self.data_cache_15sec = {}
        # live mode: stop after N items or T seconds (whichever comes first)
        self.live_max_items = live_max_items
        self.live_timeout = live_timeout

    def connect_to_db(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = False
            self.db_cursor = self.db_conn.cursor()
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise

    def load_historical_data(self):
        try:
            self.db_cursor.execute("SELECT instrument_id FROM instruments")
            instrument_ids = [row[0] for row in self.db_cursor.fetchall()]
            if not instrument_ids:
                logger.warning("No instruments loaded for indicator calculation.")
                return
            for instrument_id in instrument_ids:
                self.db_cursor.execute("""
                    SELECT time, open, high, low, close, volume, open_interest
                    FROM candle_data_1min
                    WHERE instrument_id = %s
                    ORDER BY time DESC
                    LIMIT 100
                """, (instrument_id,))
                rows = self.db_cursor.fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'open_interest'])
                    df.set_index('time', inplace=True)
                    df.sort_index(inplace=True)
                    df = df.astype({
                        'open': float, 'high': float, 'low': float,
                        'close': float, 'volume': float, 'open_interest': float
                    })
                    self.data_cache_1min[instrument_id] = df
                    logger.info(f"Loaded {len(df)} 1-minute candles for instrument {instrument_id}")
                # Also load 15s data
                self.db_cursor.execute("""
                    SELECT time, open, high, low, close, volume, open_interest
                    FROM candle_data_15sec
                    WHERE instrument_id = %s
                    ORDER BY time DESC
                    LIMIT 100
                """, (instrument_id,))
                rows_15s = self.db_cursor.fetchall()
                if rows_15s:
                    df15 = pd.DataFrame(rows_15s, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'open_interest'])
                    df15.set_index('time', inplace=True)
                    df15.sort_index(inplace=True)
                    df15 = df15.astype({
                        'open': float, 'high': float, 'low': float,
                        'close': float, 'volume': float, 'open_interest': float
                    })
                    self.data_cache_15sec[instrument_id] = df15
                    logger.info(f"Loaded {len(df15)} 15-second candles for instrument {instrument_id}")
            logger.info("Historical data loaded successfully")
        except Exception as e:
            logger.error(f"Error loading historical data: {str(e)}")
            self.db_conn.rollback()

    def calculate_obv(self, df):
        obv = 0
        obv_list = []
        closes = df['close'].values
        volumes = df['volume'].values
        for i in range(len(df)):
            if i == 0:
                obv_list.append(obv)
                continue
            if closes[i] > closes[i-1]:
                obv += volumes[i]
            elif closes[i] < closes[i-1]:
                obv -= volumes[i]
            obv_list.append(obv)
        return obv_list[-1] if obv_list else np.nan

    def calculate_rsi(self, df, period=14):
        if len(df) < period:
            return np.nan
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]

    def calculate_tvi(self, df):
        sma = df['close'].rolling(window=10).mean()
        vol_sma = df['volume'].rolling(window=10).mean()
        trend = df['close'] - sma
        tvi = trend * df['volume'] / vol_sma
        tvi_cumulative = tvi.cumsum()
        return tvi_cumulative.iloc[-1] if not tvi_cumulative.empty else np.nan

    def calculate_pvi(self, df):
        pvi = 100.0
        pvi_list = [pvi]
        closes = df['close'].values
        volumes = df['volume'].values
        for i in range(1, len(df)):
            if volumes[i] > volumes[i-1]:
                pvi = pvi + (closes[i] - closes[i-1]) / closes[i-1] * pvi
            pvi_list.append(pvi)
        return pvi_list[-1] if pvi_list else np.nan

    def calculate_pvt(self, df):
        price_change_pct = df['close'].pct_change()
        pvt = (price_change_pct * df['volume']).cumsum()
        return pvt.iloc[-1] if not pvt.empty else np.nan

    def store_indicators(self, instrument_id, timestamp, timeframe, indicators):
        try:
            self.db_cursor.execute("""
                INSERT INTO technical_indicators (time, instrument_id, timeframe, tvi, obv, rsi, pvi, pvt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, instrument_id, timeframe) DO UPDATE SET
                    tvi = EXCLUDED.tvi,
                    obv = EXCLUDED.obv,
                    rsi = EXCLUDED.rsi,
                    pvi = EXCLUDED.pvi,
                    pvt = EXCLUDED.pvt
            """, (
                timestamp, instrument_id, timeframe,
                float(indicators['tvi']) if indicators['tvi'] is not None else None,
                float(indicators['obv']) if indicators['obv'] is not None else None,
                float(indicators['rsi']) if indicators['rsi'] is not None else None,
                float(indicators['pvi']) if indicators['pvi'] is not None else None,
                float(indicators['pvt']) if indicators['pvt'] is not None else None,
            ))
            self.db_conn.commit()
            logger.debug(f"Stored indicators for instrument {instrument_id} at {timestamp} ({timeframe})")
        except Exception as e:
            logger.error(f"Error storing indicators: {str(e)}")
            self.db_conn.rollback()

    def process_data_worker(self):
        item_count = 0
        start_time = time.time()
        while self.running:
            # Stop if max items reached
            if self.live_max_items is not None and item_count >= self.live_max_items:
                logger.info(f"Reached live_max_items={self.live_max_items}, stopping live processing.")
                break
            # Stop if timeout reached
            if self.live_timeout is not None and (time.time() - start_time) >= self.live_timeout:
                logger.info(f"Reached live_timeout={self.live_timeout} seconds, stopping live processing.")
                break
            try:
                item = processed_queue.get(timeout=1)
                symbol = item.get('channel', '').rsplit('.', 1)[0]
                self.db_cursor.execute("SELECT instrument_id FROM instruments WHERE symbol=%s", (symbol,))
                res = self.db_cursor.fetchone()
                if not res:
                    continue
                instrument_id = res[0]
                self.db_cursor.execute("""
                    SELECT time, open, high, low, close, volume, open_interest
                    FROM candle_data_1min
                    WHERE instrument_id = %s
                    ORDER BY time DESC
                    LIMIT 100
                """, (instrument_id,))
                rows = self.db_cursor.fetchall()
                if not rows:
                    continue
                df = pd.DataFrame(rows, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'open_interest'])
                df.set_index('time', inplace=True)
                df.sort_index(inplace=True)
                indicators = {
                    'obv': self.calculate_obv(df),
                    'rsi': self.calculate_rsi(df),
                    'tvi': self.calculate_tvi(df),
                    'pvi': self.calculate_pvi(df),
                    'pvt': self.calculate_pvt(df)
                }
                latest_time = df.index[-1]
                self.store_indicators(instrument_id, latest_time, '1min', indicators)
                processed_queue.task_done()
                item_count += 1
            except queue.Empty:
                # If queue is empty for long, you may want to auto-stop (optional)
                if self.live_timeout is not None and (time.time() - start_time) >= self.live_timeout:
                    logger.info(f"Reached live_timeout={self.live_timeout} seconds, stopping live processing due to timeout.")
                    break
                continue
            except Exception as e:
                logger.error(f"Error in indicator processing worker: {str(e)}")
        self.stop()  # auto-stop when done

    def periodic_recalculation(self):
        try:
            logger.info("Starting periodic indicator recalculation")
            self.db_cursor.execute("SELECT instrument_id FROM instruments")
            instrument_ids = [row[0] for row in self.db_cursor.fetchall()]
            for instrument_id in instrument_ids:
                for tf, table in [('1min', 'candle_data_1min'), ('15sec', 'candle_data_15sec')]:
                    self.db_cursor.execute(f"""
                        SELECT time, open, high, low, close, volume, open_interest
                        FROM {table}
                        WHERE instrument_id = %s
                        ORDER BY time ASC
                    """, (instrument_id,))
                    rows = self.db_cursor.fetchall()
                    if not rows:
                        continue
                    df = pd.DataFrame(rows, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'open_interest'])
                    df.set_index('time', inplace=True)
                    df = df.astype({
                        'open': float, 'high': float, 'low': float,
                        'close': float, 'volume': float, 'open_interest': float
                    })
                    for timestamp in df.index:
                        dft = df.loc[:timestamp]
                        indicators = {
                            'obv': self.calculate_obv(dft),
                            'rsi': self.calculate_rsi(dft),
                            'tvi': self.calculate_tvi(dft),
                            'pvi': self.calculate_pvi(dft),
                            'pvt': self.calculate_pvt(dft)
                        }
                        self.store_indicators(instrument_id, timestamp, tf, indicators)
            logger.info("Periodic indicator recalculation completed")
        except Exception as e:
            logger.error(f"Error in periodic recalculation: {str(e)}")

    def start(self):
        self.connect_to_db()
        self.load_historical_data()
        self.running = True
        logger.info("Indicator Calculator started successfully")
        if self.mode == "batch":
            self.periodic_recalculation()
            self.stop()
        elif self.mode == "live":
            self.data_thread = threading.Thread(target=self.process_data_worker)
            self.data_thread.daemon = True
            self.data_thread.start()
            # In live mode, optionally wait for completion:
            if self.live_max_items is not None or self.live_timeout is not None:
                # Wait for the data_thread to finish
                self.data_thread.join()
        return True

    def stop(self):
        self.running = False
        if self.data_thread and self.data_thread.is_alive():
            self.data_thread.join(timeout=5)
        if self.db_conn:
            self.db_conn.close()
        logger.info("Indicator Calculator stopped")

if __name__ == "__main__":
    # Example usage:
    # For batch mode (default): python indicator_calculator.py
    # For live mode with auto-stop after 100 items or 60 seconds: 
    #   python indicator_calculator.py live 100 60
    import sys
    mode = "batch"
    live_max_items = None
    live_timeout = None
    if len(sys.argv) > 1 and sys.argv[1] == "live":
        mode = "live"
        if len(sys.argv) > 2:
            try:
                live_max_items = int(sys.argv[2])
            except Exception:
                pass
        if len(sys.argv) > 3:
            try:
                live_timeout = int(sys.argv[3])
            except Exception:
                pass
    calc = IndicatorCalculator(mode=mode, live_max_items=live_max_items, live_timeout=live_timeout)
    calc.start()