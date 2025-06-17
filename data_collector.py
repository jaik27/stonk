import os
import threading
import time
import requests
import json
import queue
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict
import psycopg2
import logging

from Socketcluster import socket as SocketClusterSocket  # <-- Use your custom socket class

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

NSE_CONFIG = {
    "loginId": os.getenv("NSE_LOGIN_ID"),
    "product": os.getenv("NSE_PRODUCT"),
    "apikey": os.getenv("NSE_API_KEY"),
    "auth_endpoint": os.getenv("NSE_AUTH_ENDPOINT"),
    "tickers_endpoint": os.getenv("NSE_TICKERS_ENDPOINT"),
    "ws_endpoint": os.getenv("NSE_WEBSOCKET_ENDPOINT")
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("NSEDataCollector")

instrument_cache = {}
data_queue = queue.Queue()

class NSEDataCollector:
    def __init__(self):
        self.socket = None
        self.access_token = None
        self.max_symbols = None
        self.db_conn = None
        self.db_cursor = None
        self.running = False
        self.last_heartbeat = time.time()
        self.subscribed_channels = []
        self.max_reconnect_attempts = 2
        self.reconnect_count = 0
        self.reconnect_delay = 5
        self.is_authenticated = False
        self.failed_channels = set()
        self.connect_to_db()
        self.load_instruments()
        self.candle_15s_buffer = defaultdict(list)
        self.last_15s_time = {}


    def connect_to_db(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = False
            self.db_cursor = self.db_conn.cursor()
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise

    def load_instruments(self):
        try:
            self.db_cursor.execute("""
                SELECT instrument_id, symbol, instrument_type, option_type, strike_price, expiry_date
                FROM instruments
            """)
            rows = self.db_cursor.fetchall()
            for row in rows:
                instrument_id, symbol, instrument_type, option_type, strike_price, expiry_date = row
                instrument_cache[symbol] = {
                    'instrument_id': instrument_id,
                    'instrument_type': instrument_type,
                    'option_type': option_type,
                    'strike_price': strike_price,
                    'expiry_date': expiry_date
                }
            logger.info(f"Loaded {len(rows)} instruments from database")
        except Exception as e:
            logger.error(f"Error loading instruments: {str(e)}")
            self.db_conn.rollback()

    def get_auth_token(self):
        auth_endpoint = f"{NSE_CONFIG['auth_endpoint']}?loginid={NSE_CONFIG['loginId']}&product={NSE_CONFIG['product']}&apikey={NSE_CONFIG['apikey']}"
        try:
            response = requests.get(auth_endpoint, timeout=10)
            if response.status_code == 200:
                logger.info(f"Full /gettoken response: {response.text}")
                response_data = response.json()
                self.access_token = (
                    response_data.get('AccessToken') 
                    or response_data.get('accesstoken')
                    or response_data.get('access_token')
                )
                self.max_symbols = int(response_data.get("MaxSymbol", 50))  # Default to 50 if not present
                if self.access_token:
                    logger.info(f"Authentication token obtained successfully (MaxSymbol={self.max_symbols})")
                    return True
                else:
                    logger.error(f"No access token in response: {response.text}")
                    return False
            else:
                logger.error(f"Failed to get auth token: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error getting auth token: {str(e)}")
            return False

    def download_tickers(self):
        if not self.access_token:
            logger.error("Access token not available")
            return False
        url = f"{NSE_CONFIG['tickers_endpoint']}?loginid={NSE_CONFIG['loginId']}&product={NSE_CONFIG['product']}&accesstoken={self.access_token}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                with open('tickers.txt', 'w') as f:
                    f.write(response.text)
                logger.info("Tickers downloaded successfully")
                return True
            else:
                logger.error(f"Failed to download tickers: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error downloading tickers: {str(e)}")
            return False

    def filter_relevant_tickers(self):
        """
        Only include tickers for allowed segments (NSE/BSE), up to MaxSymbol limit.
        Exclude MCX/OPTCOM/FUTCOM if vendor sets segment limit to 0.
        """
        try:
            tickers = []
            with open('tickers.txt', 'r') as f:
                for line in f:
                    tickers.extend([x.strip() for x in line.strip().split(',') if x.strip()])

            # Only allow NSE and BSE (adjust as needed)
            allowed_prefixes = ("NSE_", "BSE_")
            filtered = [t for t in tickers if t.startswith(allowed_prefixes)]

            # Limit to max_symbols returned by vendor
            max_allowed = self.max_symbols if self.max_symbols else 50
            relevant = filtered[:max_allowed]

            logger.info(f"Filtered {len(relevant)} relevant tickers from tickers.txt (Allowed segments: {allowed_prefixes}, MaxSymbol={max_allowed})")
            logger.info(f"Sample of tickers to be subscribed: {relevant[:10]}")
            return relevant
        except Exception as e:
            logger.error(f"Error filtering tickers: {str(e)}")
            return []

    def parse_vendor_ticker(self, ticker):
        tokens = ticker.split('_')
        d = dict(
            symbol=ticker, instrument_type=None, option_type=None, strike_price=None, expiry_date=None
        )
        if len(tokens) >= 3:
            typ = tokens[1]
            d['instrument_type'] = (
                'option' if typ.startswith('OPT') else
                'future' if typ.startswith('FUT') else
                'index' if typ == 'INDICES' else
                'stock'
            )
            if d['instrument_type'] == 'option':
                try:
                    d['option_type'] = tokens[-1]
                    d['strike_price'] = float(tokens[-2])
                    d['expiry_date'] = datetime.strptime(tokens[-3], "%d%b%Y").date()
                except Exception:
                    d['option_type'] = None
                    d['strike_price'] = None
                    d['expiry_date'] = None
            elif d['instrument_type'] == 'future':
                try:
                    d['expiry_date'] = datetime.strptime(tokens[-1], "%d%b%Y").date()
                except Exception:
                    d['expiry_date'] = None
        return d

    def update_instruments_table(self, tickers):
        try:
            for ticker in tickers:
                fields = self.parse_vendor_ticker(ticker)
                symbol = fields['symbol']
                instrument_type = fields['instrument_type']
                option_type = fields['option_type']
                strike_price = fields['strike_price']
                expiry_date = fields['expiry_date']
                if symbol not in instrument_cache:
                    self.db_cursor.execute("""
                        INSERT INTO instruments (symbol, instrument_type, option_type, strike_price, expiry_date)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (symbol) DO UPDATE SET
                            instrument_type=EXCLUDED.instrument_type,
                            option_type=EXCLUDED.option_type,
                            strike_price=EXCLUDED.strike_price,
                            expiry_date=EXCLUDED.expiry_date,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING instrument_id
                    """, (symbol, instrument_type, option_type, strike_price, expiry_date))
                    instrument_id = self.db_cursor.fetchone()[0]
                    instrument_cache[symbol] = {
                        'instrument_id': instrument_id,
                        'instrument_type': instrument_type,
                        'option_type': option_type,
                        'strike_price': strike_price,
                        'expiry_date': expiry_date
                    }
            self.db_conn.commit()
            logger.info("Instruments table updated successfully")
        except Exception as e:
            logger.error(f"Error updating instruments table: {str(e)}")
            self.db_conn.rollback()

    def subscribe_to_relevant_channels(self):
        relevant = self.filter_relevant_tickers()
        self.update_instruments_table(relevant)
        # FIX: Clear the list to avoid infinitely growing subscriptions on reconnect
        self.subscribed_channels = []

        for ticker in relevant:
            channel = f"{ticker}.JSON"  # Only one format per ticker
            try:
                self.socket.subscribeack(channel, self.on_subscription_ack)
                self.socket.onchannel(channel, self.on_channel_message)
                self.subscribed_channels.append(channel)
            except Exception as e:
                logger.error(f"Error subscribing to {channel}: {str(e)}")
        logger.info(f"Subscribed to {len(self.subscribed_channels)} channels")

    def on_connect(self, socket):
        logger.info("Connected to NSE websocket")
        self.last_heartbeat = time.time()
        self.reconnect_count = 0
        self.is_authenticated = True
        self.subscribe_to_relevant_channels()

    def on_disconnect(self, socket):
        logger.info("Disconnected from NSE websocket")
        self.is_authenticated = False
        # Optional: add a hard limit to reconnect attempts to prevent infinite loop
        if self.running and self.reconnect_count < self.max_reconnect_attempts:
            logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
            time.sleep(self.reconnect_delay)
            self.reconnect_count += 1
            self.connect_to_websocket()
        else:
            logger.error("Max reconnect attempts reached or collector stopped. Exiting reconnect loop.")
            self.running = False

    def on_connect_error(self, socket, error):
        logger.error(f"Websocket connection error: {str(error)}")
        self.reconnect_count += 1

    def on_ping(self, key, object, ack_message):
        logger.debug(f"Ping received: {object}")
        self.last_heartbeat = time.time()
        ack_message("", "#2")

    def on_subscription_ack(self, channel, error, object):
        # PATCH: Suppress or downgrade repeated subscription errors
        if error:
            if channel not in self.failed_channels:
                logger.info(f"Subscription error for {channel}: {error}")
                self.failed_channels.add(channel)
        else:
            logger.info(f"Subscription confirmed for {channel}")

    def on_channel_message(self, channel, data):
        try:
            data_obj = data
            if isinstance(data, str):
                try:
                    data_obj = json.loads(data)
                except Exception:
                    data_obj = data
            data_queue.put({
                'channel': channel,
                'data': data_obj,
                'timestamp': datetime.now()
            })
        except Exception as e:
            logger.error(f"Error processing channel message: {str(e)}")

    def process_data_worker(self):
        while self.running:
            try:
                item = data_queue.get(timeout=1)
                self.process_tick_data(item)
                data_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in data processing worker: {str(e)}")

    def process_tick_data(self, item):
        channel = item['channel']
        data = item['data']
        timestamp = item['timestamp']
        ticker = channel.rsplit('.', 1)[0]
        symbol = ticker
        if symbol not in instrument_cache:
            logger.warning(f"Instrument not found in cache: {symbol}")
            return
        instrument_id = instrument_cache[symbol]['instrument_id']

        # PATCH: Always store as UTC and timezone-aware
        if isinstance(data, dict):
            dt = None
            for cand_time_key in ['time', 'timestamp', 'datetime', 'dt']:
                if cand_time_key in data:
                    try:
                        dt = datetime.fromisoformat(data[cand_time_key])
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=time.timezone.utc)
                    except Exception:
                        dt = None
            if not dt:
                dt = timestamp
            open_, high, low, close = float(data.get('open', 0)), float(data.get('high', 0)), float(data.get('low', 0)), float(data.get('close', 0))
            volume = int(float(data.get('volume', 0)))
            oi = int(float(data.get('oi', 0))) if 'oi' in data else None
        else:
            parts = data.split(',')
            if len(parts) < 10:
                logger.warning(f"Malformed CSV: {data}")
                return
            date = parts[2]
            time_str = parts[3].zfill(4)
            dt = datetime.strptime(date + time_str, "%Y%m%d%H%M")
            dt = dt.replace(tzinfo=time.timezone.utc)
            open_, high, low, close = float(parts[4]), float(parts[5]), float(parts[6]), float(parts[7])
            volume = int(float(parts[8]))
            oi = int(float(parts[9])) if parts[9] else None

        # Aggregate 15s candles
        now = dt
        interval = now.replace(second=(now.second // 15) * 15, microsecond=0)
        buffer = self.candle_15s_buffer[symbol]
        buffer.append({'open': open_, 'high': high, 'low': low, 'close': close, 'volume': volume, 'oi': oi, 'dt': now})
        last_time = self.last_15s_time.get(symbol)
        if last_time is None:
            self.last_15s_time[symbol] = interval
        elif interval > last_time:
            self.insert_15s_candle(symbol, buffer, interval, instrument_id)
            self.candle_15s_buffer[symbol] = []
            self.last_15s_time[symbol] = interval

        try:
            self.db_cursor.execute("""
                INSERT INTO candle_data_1min (time, instrument_id, open, high, low, close, volume, open_interest)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, instrument_id) DO UPDATE SET
                    open=EXCLUDED.open,
                    high=EXCLUDED.high,
                    low=EXCLUDED.low,
                    close=EXCLUDED.close,
                    volume=EXCLUDED.volume,
                    open_interest=EXCLUDED.open_interest
            """, (dt, instrument_id, open_, high, low, close, volume, oi))
            self.db_conn.commit()
            logger.debug(f"Inserted 1min candle for {symbol} at {dt}")
        except Exception as e:
            logger.error(f"DB insert error: {str(e)}")
            self.db_conn.rollback()

    def insert_15s_candle(self, symbol, ticks, interval, instrument_id):
        if not ticks:
            return
        try:
            prices = [float(t.get('close', 0)) for t in ticks]
            open_ = float(ticks[0].get('open', 0))
            high = max(prices)
            low = min(prices)
            close = float(ticks[-1].get('close', 0))
            volume = sum(int(float(t.get('volume', 0))) for t in ticks)
            oi = ticks[-1].get('oi', None)
            self.db_cursor.execute("""
                INSERT INTO candle_data_15sec (time, instrument_id, open, high, low, close, volume, open_interest)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, instrument_id) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, open_interest=EXCLUDED.open_interest
            """, (interval, instrument_id, open_, high, low, close, volume, oi))
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to insert 15s candle for {symbol}: {e}")
            self.db_conn.rollback()

    def on_disconnect(self, socket):
        logger.info("Disconnected from NSE websocket")
        self.is_authenticated = False
        if self.running and self.reconnect_count < self.max_reconnect_attempts:
            logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
            time.sleep(self.reconnect_delay)
            self.reconnect_count += 1
            # Always refresh token before reconnect!
            if not self.get_auth_token():
                logger.error("Failed to refresh token for reconnect.")
                self.running = False
                return
            self.connect_to_websocket()
        else:
            logger.error("Max reconnect attempts reached or collector stopped. Exiting reconnect loop.")
            self.running = False

    def connect_to_websocket(self):
        if not self.access_token:
            logger.error("Access token not available")
            return False
        ws_endpoint = f"{NSE_CONFIG['ws_endpoint']}?loginid={NSE_CONFIG['loginId']}&accesstoken={self.access_token}&product={NSE_CONFIG['product']}"
        try:
            logger.info(f"Connecting to websocket: {ws_endpoint}")
            self.socket = SocketClusterSocket(ws_endpoint)
            self.socket.setBasicListener(self.on_connect, self.on_disconnect, self.on_connect_error)
            self.socket.onack('ping', self.on_ping)
            self.socket.enablelogger(True)
            self.socket.connect()
            return True
        except Exception as e:
            logger.error(f"Error connecting to websocket: {str(e)}")
            self.reconnect_count += 1
            return False

    def start(self):
        self.running = True
        if not self.get_auth_token():
            logger.error("Failed to get authentication token")
            return False
        if not self.download_tickers():
            logger.error("Failed to download tickers")
            return False
        if not self.connect_to_websocket():
            logger.error("Failed to connect to websocket")
            return False
        data_thread = threading.Thread(target=self.process_data_worker)
        data_thread.daemon = True
        data_thread.start()
        logger.info("NSE Data Collector started successfully")
        return True

    def stop(self):
        self.running = False
        if self.socket:
            self.socket.disconnect()
        if self.db_conn:
            self.db_conn.close()
        logger.info("NSE Data Collector stopped")