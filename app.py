import os
import urllib.parse
import json
from time import sleep
import threading
from datetime import datetime, timedelta
import numpy as np
import sqlite3
from tradingview_screener import Query, col, And, Or
import pandas as pd
from flask import Flask, render_template, jsonify, request
import asyncio
import json
import ssl
import threading
import websockets
import requests
import os
import signal
import configparser
from flask import Flask, redirect, request, url_for, render_template_string, jsonify, flash
from google.protobuf.json_format import MessageToDict
from datetime import datetime
# from Instruments  import get_instrument_keySpotNSE_EQ, get_instrument_keyOptions, getInstrumentsKeyFromSymbol
import configparser
import json
import time
from flask import Flask, send_from_directory
from flask_socketio import SocketIO, emit


from flask import Flask, render_template, redirect, url_for, request, session
from flask_socketio import SocketIO, emit, join_room, leave_room
import upstox_client
from threading import Thread
import requests
import os

import traceback


# Flask and Socket.IO Setup
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key_for_flask_session' # Change this in production
socketio = SocketIO(app, cors_allowed_origins="*") # Restrict origins in production




global access_token
global CLIENT_ID
global CLIENT_SECRET 
global REDIRECT_URI
global WS_SERVER_HOST 

# global access_token 
access_token = None 


# Global variables for Upstox connection
upstox_access_token = None
upstox_streamer = None
active_upstox_subscriptions = {} # Track subscribed instrument keys and client counts




def start_upstox_websocket_connection():
    global upstox_streamer

    if not upstox_access_token:
        socketio.emit('backend_status', {'message': 'Upstox Access Token not available. Please re-authenticate.'})
        return

    if upstox_streamer :#and upstox_streamer.connected:
        return # Already connected

    config = upstox_client.Configuration()
    config.access_token = upstox_access_token
    api_client = upstox_client.ApiClient(config)
    
    # Use the dedicated MarketDataStreamerV3 class
    upstox_streamer = upstox_client.MarketDataStreamerV3(api_client)

    def on_upstox_open():
        socketio.emit('backend_status', {'message': 'Connected to Upstox backend market data.'})
        # Resubscribe to instruments that were active before a potential disconnection
        for key, count in active_upstox_subscriptions.items():
            if count > 0:
                upstox_streamer.subscribe([key], "ltpc") #ltpc / full 
                socketio.emit('backend_status', {'message': f'Resubscribed to {key} on Upstox.'})

    def on_upstox_message(message):
        append_tick_to_json(message)
        if hasattr(message, 'feeds') and isinstance(message.feeds, dict):
            for instrument_key, data in message.feeds.items():
                data_dict = data.to_dict() if hasattr(data, 'to_dict') else str(data)
                socketio.emit('live_feed', {'instrument_key': instrument_key, 'data': data_dict}, room=instrument_key)
        else:
            msg_dict = message.to_dict() if hasattr(message, 'to_dict') else str(message)
            socketio.emit('live_feed', {'data': msg_dict})

    def on_upstox_close():
        socketio.emit('backend_status', {'message': 'Disconnected from Upstox backend market data.'})
        
    upstox_streamer.on("open", on_upstox_open)
    upstox_streamer.on("message", on_upstox_message)
    upstox_streamer.on("close", on_upstox_close)
    
    try:
        upstox_streamer.connect()
    except Exception as e:
        print(f"Error connecting to Upstox WebSocket: {e}")
        socketio.emit('backend_status', {'message': f'Error connecting to Upstox: {e}. Check API Key/Token.'})



# --- Utility Functions ---

def load_template_file(filename="index.html"):
    """Loads the HTML template from the file system."""
    try:
        with open('templates/'+filename, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return f"<h1>Error: Template file '{filename}' not found.</h1>"
    except Exception as e:
        return f"<h1>Error reading template: {e}</h1>"

def loadPropertiesFile():
    global CLIENT_ID
    global CLIENT_SECRET 
    global REDIRECT_URI
    global WS_SERVER_HOST
    # --- Configuration ---
    # Read credentials from my.properties
    config = configparser.ConfigParser()
    # Check if file exists, if not, create a dummy one or raise error
    if not os.path.exists('my.properties'):
        print("FATAL ERROR: 'my.properties' file not found. Create it with [DEFAULT] apikey and secret.")
        exit(1)
    config.read('my.properties')    
    CLIENT_ID = config['DEFAULT']['apikey']
    CLIENT_SECRET = config['DEFAULT']['secret']
    REDIRECT_URI = "http://127.0.0.1:8080/callback"
    WS_SERVER_HOST = '127.0.0.1'
    return True

loadPropertiesFile()



# --- Global state for auto-scanning ---
auto_scan_enabled = True
latest_scan_dfs = {
    "in_squeeze": pd.DataFrame(),
    "formed": pd.DataFrame(),
    "fired": pd.DataFrame()
}
data_lock = threading.Lock()

# --- Global state for scanner settings ---
scanner_settings = {
    "market": "india",
    "exchange": "NSE",
    "min_price": 20,
    "max_price": 10000,
    "min_volume": 50000,
    "min_value_traded": 10000000
}


import rookiepy
cookies = None
try:
    cookies = rookiepy.to_cookiejar(rookiepy.brave(['.tradingview.com']))
    print("Successfully loaded TradingView cookies.")
    #_, df = Query().select('exchange', 'update_mode').limit(1_000_000).get_scanner_data(cookies=cookies)
    #df = df.groupby('exchange')['update_mode'].value_counts()
    #print(df)

except Exception as e:
    print(f"Warning: Could not load TradingView cookies. Scanning will be disabled. Error: {e}")

# --- SQLite Timestamp Handling ---
def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 format."""
    return val.isoformat()

def convert_timestamp(val):
    """Convert ISO 8601 string to datetime.datetime object."""
    return datetime.fromisoformat(val.decode())

# Register the adapter and converter
sqlite3.register_adapter(datetime, adapt_datetime_iso)
sqlite3.register_converter("timestamp", convert_timestamp)

# --- Pandas Configuration ---
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 0)

# --- Helper Functions ---
def append_df_to_csv(df, csv_path):
    """
    Appends a DataFrame to a CSV file. Creates the file with a header if it doesn't
    exist, otherwise appends without the header.
    """
    if not os.path.exists(csv_path):
        df.to_csv(csv_path, mode='a', header=True, index=False)
    else:
        df.to_csv(csv_path, mode='a', header=False, index=False)


UpstoxWSS_JSONFilename = 'UpstoxWSS_' + datetime.now().strftime('%d_%m_%y') + '.txt'
FILEPATH = 'static/'+UpstoxWSS_JSONFilename

def append_tick_to_json( tick_data):
    """
    Appends a single tick data record to a JSON file (as a new line-delimited JSON object).

    Args:
        filename (str): The name of the JSON file.
        tick_data (dict): A dictionary containing tick information.
    """
    with open(FILEPATH, 'a') as f:
        json.dump(tick_data, f)
        f.write('\n') # Add a newline to separate JSON objects


def generate_heatmap_data(df):
    """
    Generates a simple, flat list of dictionaries from the dataframe for the D3 heatmap.
    This replaces the JSON file generation.
    """
    base_required_cols = ['ticker', 'HeatmapScore', 'SqueezeCount', 'rvol', 'URL', 'logo', 'momentum', 'highest_tf', 'squeeze_strength']
    for c in base_required_cols:
        if c not in df.columns:
            if c == 'momentum': df[c] = 'Neutral'
            elif c in ['highest_tf', 'squeeze_strength']: df[c] = 'N/A'
            else: df[c] = 0

    heatmap_data = []
    for _, row in df.iterrows():
        stock_data = {
            "name": row['ticker'], "value": row['HeatmapScore'], "count": row.get('SqueezeCount', 0),
            "rvol": row['rvol'], "url": row['URL'], "logo": row['logo'], "momentum": row['momentum'],
            "highest_tf": row['highest_tf'], "squeeze_strength": row['squeeze_strength']
        }
        if 'fired_timeframe' in df.columns: stock_data['fired_timeframe'] = row['fired_timeframe']
        if 'fired_timestamp' in df.columns and pd.notna(row['fired_timestamp']):
            stock_data['fired_timestamp'] = row['fired_timestamp'].isoformat()
        if 'previous_volatility' in df.columns: stock_data['previous_volatility'] = row['previous_volatility']
        if 'current_volatility' in df.columns: stock_data['current_volatility'] = row['current_volatility']
        if 'volatility_increased' in df.columns: stock_data['volatility_increased'] = row['volatility_increased']
        heatmap_data.append(stock_data)
    return heatmap_data

# --- Configuration ---
DB_FILE = 'squeeze_history.db'
# Timeframe Configuration
timeframes = ['', '|1', '|5', '|15', '|30', '|60', '|120', '|240', '|1W', '|1M']
tf_order_map = {'|1M': 10, '|1W': 9, '|240': 8, '|120': 7, '|60': 6, '|30': 5, '|15': 4, '|5': 3, '|1': 2, '': 1}
tf_display_map = {'': 'Daily', '|1': '1m', '|5': '5m', '|15': '15m', '|30': '30m', '|60': '1H', '|120': '2H', '|240': '4H', '|1W': 'Weekly', '|1M': 'Monthly'}
tf_suffix_map = {v: k for k, v in tf_display_map.items()}

# Construct select columns for all timeframes
select_cols = ['name', 'logoid', 'close', 'MACD.hist']
for tf in timeframes:
    select_cols.extend([
        f'KltChnl.lower{tf}', f'KltChnl.upper{tf}', f'BB.lower{tf}', f'BB.upper{tf}',
        f'ATR{tf}', f'SMA20{tf}', f'volume{tf}', f'average_volume_10d_calc{tf}', f'Value.Traded{tf}'
    ])

# --- Data Processing Functions ---
def get_highest_squeeze_tf(row):
    for tf_suffix in sorted(tf_order_map, key=tf_order_map.get, reverse=True):
        if row.get(f'InSqueeze{tf_suffix}', False): return tf_display_map[tf_suffix]
    return 'Unknown'

def get_dynamic_rvol(row, timeframe_name, tf_suffix_map):
    tf_suffix = tf_suffix_map.get(timeframe_name)
    if tf_suffix is None: return 0
    vol_col, avg_vol_col = f'volume{tf_suffix}', f'average_volume_10d_calc{tf_suffix}'
    volume, avg_volume = row.get(vol_col), row.get(avg_vol_col)
    if pd.isna(volume) or pd.isna(avg_volume) or avg_volume == 0: return 0
    return volume / avg_volume

def get_squeeze_strength(row):
    highest_tf_name = row['highest_tf']
    tf_suffix = tf_suffix_map.get(highest_tf_name)
    if tf_suffix is None: return "N/A"
    bb_upper, bb_lower = row.get(f'BB.upper{tf_suffix}'), row.get(f'BB.lower{tf_suffix}')
    kc_upper, kc_lower = row.get(f'KltChnl.upper{tf_suffix}'), row.get(f'KltChnl.lower{tf_suffix}')
    if any(pd.isna(val) for val in [bb_upper, bb_lower, kc_upper, kc_lower]): return "N/A"
    bb_width, kc_width = bb_upper - bb_lower, kc_upper - kc_lower
    if bb_width == 0: return "N/A"
    sqz_strength = kc_width / bb_width
    if sqz_strength >= 2: return "VERY STRONG"
    elif sqz_strength >= 1.5: return "STRONG"
    elif sqz_strength > 1: return "Regular"
    else: return "N/A"

def process_fired_events(events, tf_order_map, tf_suffix_map):
    if not events: return pd.DataFrame()
    df = pd.DataFrame(events)
    def get_tf_sort_key(display_name):
        suffix = tf_suffix_map.get(display_name, '')
        return tf_order_map.get(suffix, -1)
    df['tf_order'] = df['fired_timeframe'].apply(get_tf_sort_key)
    processed_events = []
    for ticker, group in df.groupby('ticker'):
        highest_tf_event = group.loc[group['tf_order'].idxmax()]
        consolidated_event = highest_tf_event.to_dict()
        consolidated_event['SqueezeCount'] = group['fired_timeframe'].nunique()
        consolidated_event['highest_tf'] = highest_tf_event['fired_timeframe']
        processed_events.append(consolidated_event)
    return pd.DataFrame(processed_events)

def get_fired_breakout_direction(row, fired_tf_name, tf_suffix_map):
    tf_suffix = tf_suffix_map.get(fired_tf_name)
    if not tf_suffix: return 'Neutral'
    close, bb_upper, kc_upper, bb_lower, kc_lower = row.get('close'), row.get(f'BB.upper{tf_suffix}'), row.get(f'KltChnl.upper{tf_suffix}'), row.get(f'BB.lower{tf_suffix}'), row.get(f'KltChnl.lower{tf_suffix}')
    if any(pd.isna(val) for val in [close, bb_upper, kc_upper, bb_lower, kc_lower]): return 'Neutral'
    if close > bb_upper and bb_upper > kc_upper: return 'Bullish'
    elif close < bb_lower and bb_lower < kc_lower: return 'Bearish'
    else: return 'Neutral'

# --- Database Functions ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS squeeze_history (id INTEGER PRIMARY KEY AUTOINCREMENT, scan_timestamp TIMESTAMP NOT NULL, ticker TEXT NOT NULL, timeframe TEXT NOT NULL, volatility REAL, rvol REAL, SqueezeCount INTEGER, squeeze_strength TEXT, HeatmapScore REAL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS fired_squeeze_events (id INTEGER PRIMARY KEY AUTOINCREMENT, fired_timestamp TIMESTAMP NOT NULL, ticker TEXT NOT NULL, fired_timeframe TEXT NOT NULL, momentum TEXT, previous_volatility REAL, current_volatility REAL, rvol REAL, HeatmapScore REAL, URL TEXT, logo TEXT, SqueezeCount INTEGER, highest_tf TEXT)''')

    # --- Schema migration: Add 'confluence' column if it doesn't exist ---
    cursor.execute("PRAGMA table_info(fired_squeeze_events)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'confluence' not in columns:
        print("Adding 'confluence' column to 'fired_squeeze_events' table.")
        cursor.execute("ALTER TABLE fired_squeeze_events ADD COLUMN confluence BOOLEAN DEFAULT 0")

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_scan_timestamp ON squeeze_history (scan_timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fired_timestamp ON fired_squeeze_events (fired_timestamp)')
    conn.commit()
    conn.close()

def load_previous_squeeze_list_from_db():
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT MAX(scan_timestamp) FROM squeeze_history')
        last_timestamp = cursor.fetchone()[0]
        if last_timestamp is None: return []
        cursor.execute('SELECT ticker, timeframe, volatility FROM squeeze_history WHERE scan_timestamp = ?', (last_timestamp,))
        return [(row[0], row[1], row[2]) for row in cursor.fetchall()]
    finally: conn.close()

def save_current_squeeze_list_to_db(squeeze_records):
    if not squeeze_records: return
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    now = datetime.now()
    data_to_insert = [(now, r['ticker'], r['timeframe'], r['volatility'], r.get('rvol'), r.get('SqueezeCount'), r.get('squeeze_strength'), r.get('HeatmapScore')) for r in squeeze_records]
    cursor.executemany('INSERT INTO squeeze_history (scan_timestamp, ticker, timeframe, volatility, rvol, SqueezeCount, squeeze_strength, HeatmapScore) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', data_to_insert)
    conn.commit()
    conn.close()

def save_fired_events_to_db(fired_events_df):
    if fired_events_df.empty: return
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    now = datetime.now()
    # Ensure 'confluence' column exists in the DataFrame, default to False if not
    if 'confluence' not in fired_events_df.columns:
        fired_events_df['confluence'] = False

    data_to_insert = [
        (now, row['ticker'], row['fired_timeframe'], row.get('momentum'),
         row.get('previous_volatility'), row.get('current_volatility'),
         row.get('rvol'), row.get('HeatmapScore'), row.get('URL'),
         row.get('logo'), row.get('SqueezeCount'), row.get('highest_tf'),
         bool(row.get('confluence', False)))  # Ensure boolean conversion
        for _, row in fired_events_df.iterrows()
    ]
    cursor = conn.cursor()
    sql = '''
        INSERT INTO fired_squeeze_events (
            fired_timestamp, ticker, fired_timeframe, momentum,
            previous_volatility, current_volatility, rvol, HeatmapScore,
            URL, logo, SqueezeCount, highest_tf, confluence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    cursor.executemany(sql, data_to_insert)
    conn.commit()
    conn.close()
    
def addListnerToFired(df ):
    try :
        for row in df:
            instrumetKey = get_instrument_keySpotNSE_EQ(row['ticker'])
            if instrumetKey :
                subscriveInstrument(instrument_key= instrumetKey)
    except :
        print("An exception occurred")
        
        traceback.print_exc()

def cleanup_old_fired_events():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    fifteen_minutes_ago = datetime.now() - timedelta(minutes=15)
    cursor.execute("DELETE FROM fired_squeeze_events WHERE fired_timestamp < ?", (fifteen_minutes_ago,))
    conn.commit()
    conn.close()

def load_recent_fired_events_from_db():
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    fifteen_minutes_ago = datetime.now() - timedelta(minutes=15)
    query = "SELECT * FROM fired_squeeze_events WHERE fired_timestamp >= ?"
    df = pd.read_sql_query(query, conn, params=(fifteen_minutes_ago,))
    conn.close()
    return df

def load_all_day_fired_events_from_db():
    """Loads all fired events from the database for the current day."""
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    query = "SELECT * FROM fired_squeeze_events WHERE fired_timestamp >= ? ORDER BY fired_timestamp DESC"
    df = pd.read_sql_query(query, conn, params=(today_start,))
    conn.close()
    # Convert timestamp to ISO format string for JSON serialization
    if not df.empty and 'fired_timestamp' in df.columns:
   #     df['fired_timestamp'] = pd.to_datetime(df['fired_timestamp']).dt.isoformat()
        # First, convert the column to datetime objects
        df['fired_timestamp'] = pd.to_datetime(df['fired_timestamp'])

        # Then, apply the isoformat() method to each element
        df['fired_timestamp'] = df['fired_timestamp'].apply(lambda x: x.isoformat())
    return df

# --- Main Scanning Logic ---
def run_scan(settings):
    """
    Runs a full squeeze scan, processes the data, saves it to the database,
    and returns the processed dataframes.
    """
    if cookies is None:
        print("Skipping scan because cookies are not loaded.")
        return {
            "in_squeeze": pd.DataFrame(),
            "formed": pd.DataFrame(),
            "fired": pd.DataFrame()
        }
    try:
        # 1. Load previous squeeze state
        prev_squeeze_pairs = load_previous_squeeze_list_from_db()

        # 2. Find all stocks currently in a squeeze
        squeeze_conditions = [And(col(f'BB.upper{tf}') < col(f'KltChnl.upper{tf}'), col(f'BB.lower{tf}') > col(f'KltChnl.lower{tf}')) for tf in timeframes]
        filters = [
            col('is_primary') == True, col('typespecs').has('common'), col('type') == 'stock',
            col('exchange') == settings['exchange'],
            col('close').between(settings['min_price'], settings['max_price']), col('active_symbol') == True,
            col('average_volume_10d_calc|5') > settings['min_volume'], col('Value.Traded|5') > settings['min_value_traded'],
            Or(*squeeze_conditions)
        ]
        query_in_squeeze = Query().select(*select_cols).where2(And(*filters)).set_markets(settings['market'])

        _, df_in_squeeze = query_in_squeeze.get_scanner_data(cookies=cookies)


        current_squeeze_pairs = []
        df_in_squeeze_processed = pd.DataFrame()
        if df_in_squeeze is not None and not df_in_squeeze.empty:
            for _, row in df_in_squeeze.iterrows():
                for tf_suffix, tf_name in tf_display_map.items():
                    if (row.get(f'BB.upper{tf_suffix}') < row.get(f'KltChnl.upper{tf_suffix}')) and (row.get(f'BB.lower{tf_suffix}') > row.get(f'KltChnl.lower{tf_suffix}')):
                        atr, sma20, bb_upper = row.get(f'ATR{tf_suffix}'), row.get(f'SMA20{tf_suffix}'), row.get(f'BB.upper{tf_suffix}')
                        volatility = (bb_upper - sma20) / atr if pd.notna(atr) and atr != 0 and pd.notna(sma20) and pd.notna(bb_upper) else 0
                        current_squeeze_pairs.append((row['ticker'], tf_name, volatility))

            df_in_squeeze['encodedTicker'] = df_in_squeeze['ticker'].apply(urllib.parse.quote)
            df_in_squeeze['URL'] = "https://in.tradingview.com/chart/N8zfIJVK/?symbol=" + df_in_squeeze['encodedTicker']
            df_in_squeeze['logo'] = df_in_squeeze['logoid'].apply(lambda x: f"https://s3-symbol-logo.tradingview.com/{x}.svg" if pd.notna(x) and x.strip() else '')
            for tf in timeframes:
                df_in_squeeze[f'InSqueeze{tf}'] = (df_in_squeeze[f'BB.upper{tf}'] < df_in_squeeze[f'KltChnl.upper{tf}']) & (df_in_squeeze[f'BB.lower{tf}'] > df_in_squeeze[f'KltChnl.lower{tf}'])
            df_in_squeeze['SqueezeCount'] = df_in_squeeze[[f'InSqueeze{tf}' for tf in timeframes]].sum(axis=1)
            df_in_squeeze['highest_tf'] = df_in_squeeze.apply(get_highest_squeeze_tf, axis=1)
            df_in_squeeze['squeeze_strength'] = df_in_squeeze.apply(get_squeeze_strength, axis=1)
            df_in_squeeze = df_in_squeeze[df_in_squeeze['squeeze_strength'].isin(['STRONG', 'VERY STRONG'])]
            df_in_squeeze['rvol'] = df_in_squeeze.apply(lambda row: get_dynamic_rvol(row, row['highest_tf'], tf_suffix_map), axis=1)
            df_in_squeeze['momentum'] = df_in_squeeze['MACD.hist'].apply(lambda x: 'Bullish' if x > 0 else 'Bearish' if x < 0 else 'Neutral')
            volatility_map = {(ticker, tf): vol for ticker, tf, vol in current_squeeze_pairs}
            df_in_squeeze['volatility'] = df_in_squeeze.apply(lambda row: volatility_map.get((row['ticker'], row['highest_tf']), 0), axis=1)
            df_in_squeeze['HeatmapScore'] = df_in_squeeze['rvol'] * df_in_squeeze['momentum'].map({'Bullish': 1, 'Neutral': 0.5, 'Bearish': -1}) * df_in_squeeze['volatility']

            ticker_data_map = {row['ticker']: row.to_dict() for _, row in df_in_squeeze.iterrows()}
            current_squeeze_records = [{'ticker': t, 'timeframe': tf, 'volatility': v, **ticker_data_map.get(t, {})} for t, tf, v in current_squeeze_pairs]
            df_in_squeeze_processed = df_in_squeeze
        else:
            current_squeeze_records = []

        # 3. Process event-based squeezes
        prev_squeeze_set = {(ticker, tf) for ticker, tf, vol in prev_squeeze_pairs}
        current_squeeze_set = {(r['ticker'], r['timeframe']) for r in current_squeeze_records}

        # --- Refactored for efficient confluence lookup ---
        prev_squeeze_state = {}
        for ticker, tf, _ in prev_squeeze_pairs:
            if ticker not in prev_squeeze_state:
                prev_squeeze_state[ticker] = set()
            prev_squeeze_state[ticker].add(tf)


        # Newly Formed
        formed_pairs = current_squeeze_set - prev_squeeze_set
        df_formed_processed = pd.DataFrame()
        if formed_pairs:
            formed_tickers = list(set(ticker for ticker, tf in formed_pairs))
            df_formed_processed = df_in_squeeze_processed[df_in_squeeze_processed['ticker'].isin(formed_tickers)].copy()

        # Newly Fired
        fired_pairs = prev_squeeze_set - current_squeeze_set
        if fired_pairs:
            fired_tickers = list(set(ticker for ticker, tf in fired_pairs))
            previous_volatility_map = {(ticker, tf): vol for ticker, tf, vol in prev_squeeze_pairs}
            query_fired = Query().select(*select_cols).set_tickers(*fired_tickers)
            _, df_fired = query_fired.get_scanner_data(cookies=cookies)

            if df_fired is not None and not df_fired.empty:
                newly_fired_events = []
                df_fired_map = {row['ticker']: row for _, row in df_fired.iterrows()}
                for ticker, fired_tf_name in fired_pairs:
                    if ticker in df_fired_map:
                        row_data = df_fired_map[ticker]
                        tf_suffix = tf_suffix_map.get(fired_tf_name)
                        if tf_suffix:
                            previous_volatility = previous_volatility_map.get((ticker, fired_tf_name), 0.0) or 0
                            atr, sma20, bb_upper = row_data.get(f'ATR{tf_suffix}'), row_data.get(f'SMA20{tf_suffix}'), row_data.get(f'BB.upper{tf_suffix}')
                            current_volatility = (bb_upper - sma20) / atr if pd.notna(atr) and atr != 0 and pd.notna(sma20) and pd.notna(bb_upper) else 0
                            if current_volatility > previous_volatility:
                                # --- Confluence Check (Efficient)---
                                has_confluence = False
                                fired_tf_rank = tf_order_map.get(tf_suffix_map.get(fired_tf_name, ''), -1)
                                if ticker in prev_squeeze_state:
                                    for prev_tf in prev_squeeze_state[ticker]:
                                        prev_tf_rank = tf_order_map.get(tf_suffix_map.get(prev_tf, ''), -1)
                                        if prev_tf_rank > fired_tf_rank:
                                            has_confluence = True
                                            break  # Found confluence

                                fired_event = row_data.to_dict()
                                fired_event.update({
                                    'fired_timeframe': fired_tf_name,
                                    'previous_volatility': previous_volatility,
                                    'current_volatility': current_volatility,
                                    'volatility_increased': True,
                                    'fired_timestamp': datetime.now(),
                                    'confluence': has_confluence
                                })
                                newly_fired_events.append(fired_event)
                if newly_fired_events:
                    df_newly_fired = process_fired_events(newly_fired_events, tf_order_map, tf_suffix_map)
                    df_newly_fired['URL'] = "https://in.tradingview.com/chart/N8zfIJVK/?symbol=" + df_newly_fired['ticker'].apply(urllib.parse.quote)
                    df_newly_fired['logo'] = df_newly_fired['logoid'].apply(lambda x: f"https://s3-symbol-logo.tradingview.com/{x}.svg" if pd.notna(x) and x.strip() else '')
                    df_newly_fired['rvol'] = df_newly_fired.apply(lambda row: get_dynamic_rvol(row, row['highest_tf'], tf_suffix_map), axis=1)
                    df_newly_fired['momentum'] = df_newly_fired.apply(lambda row: get_fired_breakout_direction(row, row['highest_tf'], tf_suffix_map), axis=1)
                    # Set squeeze_strength based on confluence
                    df_newly_fired['squeeze_strength'] = np.where(df_newly_fired['confluence'], 'FIRED (Confluence)', 'FIRED')
                    df_newly_fired['HeatmapScore'] = df_newly_fired['rvol'] * df_newly_fired['momentum'].map({'Bullish': 1, 'Neutral': 0.5, 'Bearish': -1}) * df_newly_fired['current_volatility']
                    save_fired_events_to_db(df_newly_fired)
                    
                    append_df_to_csv(df_newly_fired, 'BBSCAN_FIRED_' + datetime.now().strftime('%d_%m_%y') + '.csv')

        # 4. Consolidate and prepare final data
        # cleanup_old_fired_events() # Disabled to keep all day's events
        df_recent_fired = load_recent_fired_events_from_db()
        if not df_recent_fired.empty:
            fired_events_list = df_recent_fired.to_dict('records')
            addListnerToFired(fired_events_list)
            df_recent_fired_processed = process_fired_events(fired_events_list, tf_order_map, tf_suffix_map)
        else:
            df_recent_fired_processed = pd.DataFrame()


        # 5. Save current state
        save_current_squeeze_list_to_db(current_squeeze_records)

        # 6. Return processed dataframes
        return {
            "in_squeeze": df_in_squeeze_processed,
            "formed": df_formed_processed,
            "fired": df_recent_fired_processed
        }

    except Exception as e:
        print(f"An error occurred during scan: {e}")
        return {
            "in_squeeze": pd.DataFrame(),
            "formed": pd.DataFrame(),
            "fired": pd.DataFrame()
        }

def background_scanner():
    """Function to run scans in the background."""
    while True:
        if auto_scan_enabled:
            print("Auto-scanning...")
            with data_lock:
                current_settings = scanner_settings.copy()
            scan_result_dfs = run_scan(current_settings)
            with data_lock:
                global latest_scan_dfs
                latest_scan_dfs = scan_result_dfs
        sleep(60)

def get_upstox_access_token(code):
    """Exchanges authorization code for access token."""
    url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {'accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {
        'code': code, 'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI, 'grant_type': 'authorization_code'
    }
    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.exceptions.RequestException as e:
        print(f"Error getting access token: {e}")
        return None
    
# --- Flask Routes ---
@app.route('/')
def index():
    return render_template('SqueezeHeatmap.html')

@app.route('/WSS')
def wss_output():
    return render_template('WSS_OUTOUT.html')

@app.route('/BubbleChart')
def bubbleChart_output():
    return render_template('BubbleChart.html')


@app.route('/fired')
def fired_page():
    return render_template('Fired.html')

@app.route('/formed')
def formed_page():
    return render_template('Formed.html')

@app.route('/compact')
def compact_page():
    return render_template('CompactHeatmap.html')

@app.route('/scan', methods=['POST'])
def scan_endpoint():
    """Triggers a new scan and returns the filtered results."""
    rvol_threshold = request.json.get('rvol', 0) if request.json else 0
    with data_lock:
        current_settings = scanner_settings.copy()
    scan_results = run_scan(current_settings)

    if rvol_threshold > 0:
        for key in scan_results:
            if not scan_results[key].empty:
                scan_results[key] = scan_results[key][scan_results[key]['rvol'] > rvol_threshold]

    response_data = {
        "in_squeeze": generate_heatmap_data(scan_results["in_squeeze"]),
        "formed": generate_heatmap_data(scan_results["formed"]),
        "fired": generate_heatmap_data(scan_results["fired"])
    }
    return jsonify(response_data)

@app.route('/get_latest_data', methods=['GET'])
def get_latest_data():
    """Returns the latest cached scan data, with optional RVOL filtering."""
    rvol_threshold = request.args.get('rvol', default=0, type=float)

    with data_lock:
        # Make a copy to work with
        dfs = {
            "in_squeeze": latest_scan_dfs["in_squeeze"].copy(),
            "formed": latest_scan_dfs["formed"].copy(),
            "fired": latest_scan_dfs["fired"].copy()
        }

    # Apply RVOL filter
    if rvol_threshold > 0:
        for key in dfs:
            if not dfs[key].empty:
                dfs[key] = dfs[key][dfs[key]['rvol'] > rvol_threshold]

    # Generate JSON response
    response_data = {
        "in_squeeze": generate_heatmap_data(dfs["in_squeeze"]),
        "formed": generate_heatmap_data(dfs["formed"]),
        "fired": generate_heatmap_data(dfs["fired"])
    }
    return jsonify(response_data)

@app.route('/toggle_scan', methods=['POST'])
def toggle_scan():
    global auto_scan_enabled
    data = request.get_json()
    auto_scan_enabled = data.get('enabled', auto_scan_enabled)
    return jsonify({"status": "success", "auto_scan_enabled": auto_scan_enabled})

@app.route('/get_all_fired_events', methods=['GET'])
def get_all_fired_events():
    """Returns all fired squeeze events for the current day."""
    fired_events_df = load_all_day_fired_events_from_db()
    return jsonify(fired_events_df.to_dict('records'))

@app.route('/update_settings', methods=['POST'])
def update_settings():
    """Updates the global scanner settings."""
    global scanner_settings
    new_settings = request.get_json()
    with data_lock:
        for key, value in new_settings.items():
            if key in scanner_settings:
                # Basic type casting for robustness
                try:
                    if isinstance(scanner_settings[key], int):
                        scanner_settings[key] = int(value)
                    elif isinstance(scanner_settings[key], float):
                        scanner_settings[key] = float(value)
                    else:
                        scanner_settings[key] = value
                except (ValueError, TypeError):
                    # Keep original value if casting fails
                    pass
    return jsonify({"status": "success", "settings": scanner_settings})



@app.route("/login")
def login():
    """Redirects user to the Upstox OAuth login page."""
    auth_url = (
        f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&"
        f"client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    )
    return redirect(auth_url)

@app.route('/callback')
def callback():
    global upstox_access_token ,access_token
    code = request.args.get('code')
    if code:
        upstox_access_token = get_upstox_access_token(code)
        if upstox_access_token:
            session['upstox_access_token'] = upstox_access_token
            access_token = upstox_access_token
            if not upstox_streamer :#or not upstox_streamer.connected:
                Thread(target=start_upstox_websocket_connection, daemon=True).start()
            return redirect(url_for('index'))
        else:
            return "Failed to get access token. Check server logs.", 500
    return "Authorization code not found.", 400

# @app.route("/callback")
# def callback():
#     """Handles the OAuth callback, exchanges code for access token, and starts WSS."""
#     global access_token
#     global CLIENT_ID
#     global CLIENT_SECRET
#     global REDIRECT_URI
#     code = request.args.get('code')
    
#     if not code:
#         return "<h1>Authorization failed. Missing code parameter.</h1>"

#     token_url = "https://api.upstox.com/v2/login/authorization/token"
#     headers = {'accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'}
#     data = {
#         'code': code,
#         'client_id': CLIENT_ID,
#         'client_secret': CLIENT_SECRET,
#         'redirect_uri': REDIRECT_URI,
#         'grant_type': 'authorization_code'
#     }
    
#     try:
#         response = requests.post(token_url, headers=headers, data=data)
#         response.raise_for_status()
#         token_data = response.json()
        
#         access_token = token_data.get('access_token')
        
#         if not access_token:
#             return f"<h1>Token exchange failed:</h1><pre>{json.dumps(token_data, indent=2)}</pre>", 500
            
#         # Start the thread that runs the asyncio loop for WSS and local server
         
#         return redirect(url_for('index'))

#     except requests.exceptions.RequestException as e:
#         return f"<h1>Error during token exchange:</h1><pre>{e}</pre>", 500

@app.route('/upstox')
def upstox(): 
    propertiesFileLoaded = loadPropertiesFile()
    html_template = load_template_file("upstox_login.html")
     
    token_status = "Available" if access_token else "Missing (Please Log In)"
    
    
    message = "Properties FILE  "
    if propertiesFileLoaded :
        message = message +" LOADED SUCCESSFULLY"
    else :
        message = message +" NOT LOADED !!! ERROR !!! "
    
    redirectToIndex = access_token and propertiesFileLoaded
    if redirectToIndex :
        # Flash a message to be displayed on the page
        flash('Redirecting to index in 3 seconds...', 'info')
        
    
    
    return render_template_string(
        html_template,          
        token_status=token_status,
        message=message,
        redirectToIndex = redirectToIndex
        
    )
    

#HELPER FUNCTION 
import pandas as pd  
file_path = 'instruments.csv'
if os.path.exists(file_path):
    print(f"The file '{file_path}' exists.")
    instruments = pd.read_csv(file_path)
else:
    print(f"The file '{file_path}' does not exist.")
    df=pd.read_csv('https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz')
    instruments = df[df['instrument_key'].str.startswith('NSE_EQ')]
    instruments.to_csv(file_path,index=False)




# def getInstrumentsKeyFromSymbol(symbol):
    
    
#     return ""

def get_instrument_keyOptions(spotsymbol,strike,option_type):
	
	instruments['tradingsymbol']=instruments['tradingsymbol'].fillna('')
	instrument_key=instruments[(instruments['tradingsymbol'].str.contains(spotsymbol))
	&(instruments['tradingsymbol'].str.contains(strike))
		&(instruments['tradingsymbol'].str.contains(option_type))].sort_values('expiry')
	return instrument_key['instrument_key'].iloc[0]

def get_instrument_keySpotNSE_EQ(NSE_spotsymbol):
    spltSymb = str.split(NSE_spotsymbol,":")
    spotsymbol = spltSymb[1].strip()
    # instruments=pd.read_csv('instruments.csv')
    instruments['tradingsymbol']=instruments['tradingsymbol'].fillna('')
    instrument_key =instruments[(instruments['tradingsymbol'] == spotsymbol) 	&(instruments['instrument_key'].str.contains("NSE_EQ"))]
    #   
    #print(instrument_key['instrument_key'])
    if instrument_key.size > 0 :
        # print(instrument_key['instrument_key'].iloc[0])
        return instrument_key['instrument_key'].iloc[0]
    else :
        print("No Symbol found for "+spotsymbol)
        return  None

















# Data variables (shared between file loading and live simulation)
file_data_cache = {} # Stores all parsed messages from the loaded file
available_securities = []
last_trade_price = {} # {security_id: price}

# Aggregation variables (simulating real-time server-side calculation)
ohlc_data = {}         # {security_id: [bucket1, bucket2, ...]}
trades_by_exact_time = {} # {security_id: {time_ms: {trades: [...], price: 100}}}

# User-defined parameters (would normally be managed per session/user)
CURRENT_INTERVAL_MS = 60000 
THRESHOLD_QTY = 20
BIG_PLAYER_THRESHOLD = 50
GLOBAL_AVG_LTQ = 50 

def get_interval_ms(interval_str):
    """Converts interval string (e.g., '10S', '1T') to milliseconds."""
    if not interval_str: return 60000
    if interval_str.endswith('S'):
        return int(interval_str[:-1]) * 1000
    elif interval_str.endswith('T'):
        return int(interval_str[:-1]) * 60000
    return 60000

def get_empty_bucket(time_ms, price):
    """Creates a new OHLC bucket."""
    return {
        'time': time_ms,
        'open': price, 'high': price, 'low': price, 'close': price,
        'buyVolume': 0, 'bigPlayerBuyVolume': 0, 'sellVolume': 0, 'bigPlayerSellVolume': 0,
    }

# --- Core Data Aggregation Logic (Refactored to Python) ---

def process_single_trade(security_id, feed):
    """Processes a single trade feed and updates OHLC/Bubble data."""
    global last_trade_price
    global ohlc_data
    global trades_by_exact_time
    
    if not (feed and feed.get('ltpc') and security_id in ohlc_data):
        return

    ltpc = feed['ltpc']
    time_ms = int(ltpc['ltt'])
    new_price = float(ltpc['ltp'])
    quantity = int(ltpc['ltq'])
    
    current_ohlc_list = ohlc_data[security_id]['ohlc']
    current_bubble_list = ohlc_data[security_id]['bubble']
    
    # Determine aggressor
    aggressor = 'HOLD'
    if security_id in last_trade_price:
        if new_price > last_trade_price[security_id]: aggressor = 'BUY'
        elif new_price < last_trade_price[security_id]: aggressor = 'SELL'

    trade = {'time': time_ms, 'price': new_price, 'quantity': quantity, 'aggressor': aggressor}

    # 1. Bubble Data Aggregation (Trades at same timestamp)
    if security_id not in trades_by_exact_time:
        trades_by_exact_time[security_id] = {}
        
    if time_ms not in trades_by_exact_time[security_id]:
        trades_by_exact_time[security_id][time_ms] = {'trades': [], 'price': new_price}
        
    trades_by_exact_time[security_id][time_ms]['trades'].append(trade)

    current_group = trades_by_exact_time[security_id][time_ms]
    sum_ltq = sum(t['quantity'] for t in current_group['trades'])
    max_ltq = max(t['quantity'] for t in current_group['trades'])

    if sum_ltq >= THRESHOLD_QTY and max_ltq >= BIG_PLAYER_THRESHOLD:
        impact_score = max_ltq / GLOBAL_AVG_LTQ
        
        # Check if bubble already exists for this exact time
        if not any(b['x'] == time_ms for b in current_bubble_list):
            current_bubble_list.append({
                'x': time_ms,
                'y': new_price,
                'q': sum_ltq,
                'impactScore': impact_score,
            })

    # 2. OHLC Candle Aggregation
    interval_start_ms = (time_ms // CURRENT_INTERVAL_MS) * CURRENT_INTERVAL_MS
    
    current_bucket = current_ohlc_list[-1] if current_ohlc_list else None

    if not current_bucket or current_bucket['time'] != interval_start_ms:
        # New interval started: start a new bucket
        new_bucket = get_empty_bucket(interval_start_ms, new_price)
        current_ohlc_list.append(new_bucket)
        current_bucket = new_bucket
    
    # Update current bucket
    current_bucket['close'] = new_price
    current_bucket['high'] = max(current_bucket['high'], new_price)
    current_bucket['low'] = min(current_bucket['low'], new_price)

    # Volume aggregation
    is_big_player = quantity >= BIG_PLAYER_THRESHOLD

    if aggressor == 'BUY' or aggressor == 'HOLD':
        if is_big_player: current_bucket['bigPlayerBuyVolume'] += quantity
        else: current_bucket['buyVolume'] += quantity
    else: 
        if is_big_player: current_bucket['bigPlayerSellVolume'] += quantity
        else: current_bucket['sellVolume'] += quantity
    
    last_trade_price[security_id] = new_price
    
    # Return the last (current) bucket for quick live update
    return current_bucket, current_bubble_list

# --- Initial Load & Setup ---

def load_file_data():
    """Loads and preprocesses the historical data file."""
    global file_data_cache
    global available_securities
    print(f"Server: Attempting to load file from {FILEPATH}")
    
    try:
        with open(FILEPATH, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    if data.get('feeds'):
                        for sec_id in data['feeds'].keys():
                            if sec_id not in file_data_cache:
                                file_data_cache[sec_id] = []
                            file_data_cache[sec_id].append(data)
                            
                except json.JSONDecodeError:
                    continue # Skip invalid JSON lines
        
        available_securities = sorted(file_data_cache.keys())
        print(f"Server: File loaded successfully. Found {len(available_securities)} securities.")
        return True
        
    except FileNotFoundError:
        print(f"Server: WARNING! Historical file not found at {FILEPATH}. Running in pure simulated live mode.")
        available_securities = ['NSE_EQ|INE002A01018', 'NSE_EQ|INE019A01017', 'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA'] # Default live list
        return False

def aggregate_historical_data(security_id, interval_ms):
    """Aggregates all historical data for a given security and interval."""
    global ohlc_data
    global CURRENT_INTERVAL_MS
    CURRENT_INTERVAL_MS = interval_ms
    
    # Reset aggregation for this security
    ohlc_data[security_id] = {'ohlc': [], 'bubble': []}
    
    if security_id not in file_data_cache:
        print(f"Server: No cached data for {security_id}. Starting empty.")
        return

    # Process all historical messages
    for message in file_data_cache[security_id]:
        feed = message.get('feeds', {}).get(security_id)
        if feed and feed.get('ltpc'):
            process_single_trade(security_id, feed)

    print(f"Server: Historical aggregation complete for {security_id}. {len(ohlc_data[security_id]['ohlc'])} candles generated.")
    
    # Convert internal dict format to ECharts array format for sending to client
    candles = ohlc_data[security_id]['ohlc']
    
    ec_ohlc = [[c['time'], c['open'], c['close'], c['low'], c['high']] for c in candles]
    ec_normal_buy = [[c['time'], c['buyVolume']] for c in candles]
    ec_big_buy = [[c['time'], c['bigPlayerBuyVolume']] for c in candles]
    ec_normal_sell = [[c['time'], -c['sellVolume']] for c in candles]
    ec_big_sell = [[c['time'], -c['bigPlayerSellVolume']] for c in candles]
    
    return {
        'ohlc': ec_ohlc,
        'normalBuy': ec_normal_buy,
        'bigBuy': ec_big_buy,
        'normalSell': ec_normal_sell,
        'bigSell': ec_big_sell,
        'bubble': ohlc_data[security_id]['bubble']
    }


 

# --- Socket.IO Event Handlers ---

# Socket.IO Events
@socketio.on('connect')
def handle_client_connect():
    emit('backend_status', {'message': 'Connected to backend server.'})

@socketio.on('disconnect')
def handle_client_disconnect():
    for key in list(active_upstox_subscriptions.keys()):
        if request.sid in socketio.server.rooms(request.sid):
            leave_room(key, sid=request.sid)
            active_upstox_subscriptions[key] -= 1
            if active_upstox_subscriptions[key] <= 0:
                if upstox_streamer :#and upstox_streamer.connected:
                    print( " REMOVED upstox_streamer.unsubscribe([key]) ")
                del active_upstox_subscriptions[key]
        
         

        
def subscriveInstrument(instrument_key) :
        
    if instrument_key and upstox_streamer :#and upstox_streamer.connected:
        # join_room(instrument_key)
        active_upstox_subscriptions[instrument_key] = active_upstox_subscriptions.get(instrument_key, 0) + 1
        if active_upstox_subscriptions[instrument_key] == 1:
            upstox_streamer.subscribe([instrument_key], "ltpc")
        #     emit('backend_status', {'message': f'Backend subscribing to {instrument_key} from Upstox.'})
        # emit('subscription_status', {'instrument_key': instrument_key, 'status': 'subscribed'}, room=request.sid)
    else:
        status_msg = 'Backend not ready or invalid key'
        if not upstox_streamer: status_msg = 'Upstox streamer not initialized'
        else  : status_msg = 'Upstox streamer not connected'
        # emit('subscription_status', {'instrument_key': instrument_key, 'status': 'failed', 'error': status_msg}, room=request.sid)



@socketio.on('subscribe_instrument')
def handle_subscribe(data):
    instrument_key = data.get('instrument_key')
    if instrument_key and upstox_streamer :#and upstox_streamer.connected:
        join_room(instrument_key)
        active_upstox_subscriptions[instrument_key] = active_upstox_subscriptions.get(instrument_key, 0) + 1
        if active_upstox_subscriptions[instrument_key] == 1:
            upstox_streamer.subscribe([instrument_key], "ltpc")
            emit('backend_status', {'message': f'Backend subscribing to {instrument_key} from Upstox.'})
        emit('subscription_status', {'instrument_key': instrument_key, 'status': 'subscribed'}, room=request.sid)
    else:
        status_msg = 'Backend not ready or invalid key'
        if not upstox_streamer: status_msg = 'Upstox streamer not initialized'
        else  : status_msg = 'Upstox streamer not connected'
        emit('subscription_status', {'instrument_key': instrument_key, 'status': 'failed', 'error': status_msg}, room=request.sid)

    
@socketio.on('unsubscribe_instrument')
def handle_unsubscribe(data):
    instrument_key = data.get('instrument_key')
    if instrument_key and upstox_streamer :#and upstox_streamer.connected:
        if instrument_key in active_upstox_subscriptions:
            leave_room(instrument_key)
            active_upstox_subscriptions[instrument_key] -= 1
            if active_upstox_subscriptions[instrument_key] <= 0:
                print( "REMOVED !!!! ALERT  upstox_streamer.unsubscribe([instrument_key]) ")
                emit('backend_status', {'message': f'Backend unsubscribing from {instrument_key} on Upstox.'})
                del active_upstox_subscriptions[instrument_key]
            emit('subscription_status', {'instrument_key': instrument_key, 'status': 'unsubscribed'}, room=request.sid)
    else:
        status_msg = 'Backend not ready or invalid key'
        if not upstox_streamer: status_msg = 'Upstox streamer not initialized'
        else : status_msg = 'Upstox streamer not connected'
        emit('subscription_status', {'instrument_key': instrument_key, 'status': 'failed', 'error': status_msg}, room=request.sid)





@socketio.on('get_securities')
def handle_get_securities():
    """Sends the list of available securities to the client."""
    emit('available_securities', {'securities': available_securities})
    print(f"Server: Sent {len(available_securities)} securities list to client.")

@socketio.on('start_stream')
def handle_start_stream(data):
    """
    1. Runs historical aggregation.
    2. Sends the aggregated data to the client.
    3. Starts the live simulation thread.
    """
    security_id = data['securityId']
    interval_str = data['interval']
    global THRESHOLD_QTY
    global BIG_PLAYER_THRESHOLD
    
    # Update global settings from client
    THRESHOLD_QTY = int(data.get('threshold', THRESHOLD_QTY))
    BIG_PLAYER_THRESHOLD = int(data.get('bigPlayerThreshold', BIG_PLAYER_THRESHOLD))
    CURRENT_INTERVAL_MS = get_interval_ms(interval_str)
    
    print(f"Server: Starting stream for {security_id} ({interval_str}).")
    
    # 1. Aggregate and send historical data
    aggregated_data = aggregate_historical_data(security_id, CURRENT_INTERVAL_MS)
    emit('historical_data', {'securityId': security_id, 'data': aggregated_data})
    print(f"Server: Sent historical data for {security_id}.")
    
    # 2. Start live simulation (The thread simulates receiving new 'live' trade feeds)
    # This simulation uses the rest of the trades in the cache that weren't used for historical data
    socketio.start_background_task(live_trade_simulator, security_id)

    # *** CRITICAL: Use the Socket.IO method to start the thread ***
    socketio.start_background_task(live_trade_simulator, security_id)
    # DO NOT use threading.Thread(target=live_trade_simulator, args=(security_id,)).start()

# If you are using `threading.Thread` directly, you must manually push the context at the start of the thread's execution, which is complex. Using `socketio.start_background_task` is the preferred and supported way to handle this.

# If the error continues, the next most likely culprit is an `emit` call in your `mock_live_feed` function or another background process that's missing the `with app.app_context():` wrapper.




def live_trade_simulator(security_id):
    """Simulates a real-time feed by iterating over the file data."""
    print(f"Server: Starting live simulation thread for {security_id}...")
    
    if security_id not in file_data_cache or not file_data_cache[security_id]:
        print(f"Server: No file data to simulate for {security_id}. Simulating mock trades.")
        mock_live_feed(security_id)
        return
        
    trades_to_simulate = file_data_cache[security_id]
    
    # NOTE: The trades were already processed in aggregate_historical_data, 
    # but since this is a simulation, we re-process them one-by-one to mimic live arrival.
    
    for message in trades_to_simulate:
        socketio.sleep(0.01) # Small delay to simulate network latency
        
        # Check if the client is still connected and interested in this security
        if security_id not in ohlc_data:
            print(f"Server: Stream stopped for {security_id}.")
            break

        feed = message.get('feeds', {}).get(security_id)
        if feed and feed.get('ltpc'):
            try:
                # Re-run aggregation for the single trade (it will update the LAST candle)
                current_bucket, current_bubble_list = process_single_trade(security_id, feed)
                
                # FIX: Wrap emit in application context for background thread safety
                with app.app_context():
                    # Send ONLY the *last* OHLC bucket and *all* bubble points for live update
                    socketio.emit('live_update', {
                        'securityId': security_id,
                        'lastCandle': current_bucket,
                        'bubbles': current_bubble_list
                    })
            except Exception as e:
                print(f"Error in simulation for {security_id}: {e}")
                # Use traceback.print_exc() for detailed error logging in the server console
                traceback.print_exc()
                continue


def mock_live_feed(security_id):
    """Generates mock live data when no file is loaded."""
    if security_id not in ohlc_data:
        ohlc_data[security_id] = {'ohlc': [], 'bubble': []}

    base_price = 100.0
    last_price = base_price
    
    for i in range(200):
        socketio.sleep(1) 
        
        if security_id not in ohlc_data:
            break
            
        now_ms = int(time.time() * 1000)
        # [ ... trade generation logic ... ]
        
        feed = mock_message['feeds'][security_id]
        
        try:
            current_bucket, current_bubble_list = process_single_trade(security_id, feed)
            
            # --- FIX APPLIED HERE ---
            with app.app_context():
                socketio.emit('live_update', {
                    'securityId': security_id,
                    'lastCandle': current_bucket,
                    'bubbles': current_bubble_list
                }, broadcast=True)
            # --- FIX END ---
            
        except Exception as e:
            print(f"Error in mock stream for {security_id}: {e}")
            traceback.print_exc()
            
        last_price = new_price

 

# --- Flask Routes ---

@app.route('/BubbleChart')
def serve_index():
    return render_template('BubbleChart.html')

@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serves the static files (like the data file) from the static directory."""
    return send_from_directory('static', filename)



if __name__ == '__main__':
    init_db()
    # Start the background scanner thread
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    # app.run(debug=True, port=8080)
     # We will pretend the file is there for now for the server logic to work
    import os
    if not os.path.exists('static'):
        os.makedirs('static')
    
    load_file_data()

    socketio.run(app, debug=True, allow_unsafe_werkzeug=True, port=8080)


# To run this server: 
# 1. Save this as server.py
# 2. Create a folder named 'static' in the same directory.
# 3. Save your data file (UpstoxWSS_07_10_25.txt) inside the 'static' folder.
# 4. Install dependencies: pip install flask flask-socketio simple-websocket
# 5. Run: python server.py
 