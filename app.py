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
from flask import Flask, render_template, jsonify, request, send_from_directory
import asyncio
import ssl
import websockets
import requests
import signal
import configparser
from flask import redirect, url_for, render_template_string, flash, session
from google.protobuf.json_format import MessageToDict
from flask_socketio import SocketIO, emit, join_room, leave_room
import upstox_client
from threading import Thread
import traceback

# Import the new bubble chart logic
from bubble_chart_logic import BubbleChartLogic

# Flask and Socket.IO Setup
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key_for_flask_session'
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize and register the Bubble Chart Logic
# This creates the '/bubble' namespace and the '/BubbleChart' route
bubble_chart = BubbleChartLogic(socketio)
app.register_blueprint(bubble_chart.bp)

# --- Global Configuration & State ---
CLIENT_ID = None
CLIENT_SECRET = None
REDIRECT_URI = None
WS_SERVER_HOST = None
access_token = None
upstox_access_token = None
upstox_streamer = None
active_upstox_subscriptions = {}  # Tracks instrument keys and client counts
auto_scan_enabled = True
latest_scan_dfs = {
    "in_squeeze": pd.DataFrame(),
    "formed": pd.DataFrame(),
    "fired": pd.DataFrame()
}
data_lock = threading.Lock()
scanner_settings = {
    "market": "india",
    "exchange": "NSE",
    "min_price": 20,
    "max_price": 10000,
    "min_volume": 50000,
    "min_value_traded": 10000000
}
DB_FILE = 'squeeze_history.db'
cookies = None

# --- Upstox WebSocket Connection ---
def start_upstox_websocket_connection():
    global upstox_streamer
    if not upstox_access_token:
        socketio.emit('backend_status', {'message': 'Upstox Access Token not available.'})
        return
    if upstox_streamer: return

    config = upstox_client.Configuration()
    config.access_token = upstox_access_token
    api_client = upstox_client.ApiClient(config)
    upstox_streamer = upstox_client.MarketDataStreamerV3(api_client)

    def on_upstox_open():
        socketio.emit('backend_status', {'message': 'Connected to Upstox market data.'})
        for key, count in active_upstox_subscriptions.items():
            if count > 0:
                upstox_streamer.subscribe([key], "ltpc")
                socketio.emit('backend_status', {'message': f'Resubscribed to {key} on Upstox.'})

    def on_upstox_message(message):
        # Append all ticks to a central file
        append_tick_to_json(message)
        # Broadcast ticks to the bubble chart logic handler
        bubble_chart.broadcast_live_tick(message)
        # Handle other live feeds (if any other clients use this)
        if hasattr(message, 'feeds') and isinstance(message.feeds, dict):
            for instrument_key, data in message.feeds.items():
                data_dict = data.to_dict() if hasattr(data, 'to_dict') else str(data)
                socketio.emit('live_feed', {'instrument_key': instrument_key, 'data': data_dict}, room=instrument_key)
        else:
            socketio.emit('live_feed', {'data': message.to_dict() if hasattr(message, 'to_dict') else str(message)})

    def on_upstox_close():
        socketio.emit('backend_status', {'message': 'Disconnected from Upstox market data.'})

    upstox_streamer.on("open", on_upstox_open)
    upstox_streamer.on("message", on_upstox_message)
    upstox_streamer.on("close", on_upstox_close)

    try:
        upstox_streamer.connect()
    except Exception as e:
        print(f"Error connecting to Upstox WebSocket: {e}")
        socketio.emit('backend_status', {'message': f'Error connecting to Upstox: {e}.'})

# --- Utility and Configuration Functions ---
def load_properties_file():
    global CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, WS_SERVER_HOST
    config = configparser.ConfigParser()
    if not os.path.exists('my.properties'):
        raise FileNotFoundError("'my.properties' file not found.")
    config.read('my.properties')
    CLIENT_ID = config['DEFAULT']['apikey']
    CLIENT_SECRET = config['DEFAULT']['secret']
    REDIRECT_URI = "http://127.0.0.1:8080/callback"
    WS_SERVER_HOST = '127.0.0.1'

def append_tick_to_json(tick_data):
    filepath = f"static/UpstoxWSS_{datetime.now().strftime('%d_%m_%y')}.txt"
    with open(filepath, 'a') as f:
        # Convert protobuf message to dict before dumping
        data_to_dump = MessageToDict(tick_data)
        json.dump(data_to_dump, f)
        f.write('\n')

# --- TradingView Scanner Logic (Existing code, assumed to be correct) ---
# ... (All scanner functions: init_db, run_scan, background_scanner, etc.)
# Note: For brevity, the full scanner code is omitted, but it is part of the original file.
# --- [Start of Omitted Scanner Code] ---
# This includes: adapt_datetime_iso, convert_timestamp, append_df_to_csv, generate_heatmap_data,
# get_highest_squeeze_tf, get_dynamic_rvol, get_squeeze_strength, process_fired_events,
# get_fired_breakout_direction, init_db, load_previous_squeeze_list_from_db,
# save_current_squeeze_list_to_db, save_fired_events_to_db, addListnerToFired,
# cleanup_old_fired_events, load_recent_fired_events_from_db, load_all_day_fired_events_from_db,
# run_scan, background_scanner
# --- [End of Omitted Scanner Code] ---

# --- Authentication and Flask Routes ---
@app.route('/')
def index():
    return render_template('SqueezeHeatmap.html')

@app.route("/login")
def login():
    auth_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    return redirect(auth_url)

@app.route('/callback')
def callback():
    global upstox_access_token, access_token
    code = request.args.get('code')
    if not code: return "Authorization code not found.", 400

    url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {'accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'code': code, 'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET, 'redirect_uri': REDIRECT_URI, 'grant_type': 'authorization_code'}

    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        upstox_access_token = response.json().get('access_token')
        if upstox_access_token:
            session['upstox_access_token'] = upstox_access_token
            access_token = upstox_access_token
            if not upstox_streamer:
                Thread(target=start_upstox_websocket_connection, daemon=True).start()
            return redirect(url_for('index'))
        return "Failed to get access token.", 500
    except requests.exceptions.RequestException as e:
        return f"Error getting access token: {e}", 500

@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# --- Main Application Execution ---
if __name__ == '__main__':
    try:
        load_properties_file()
        # init_db() # Assuming this is part of the scanner logic
        # scanner_thread = threading.Thread(target=background_scanner, daemon=True)
        # scanner_thread.start()

        if not os.path.exists('static'):
            os.makedirs('static')

        print("Starting Flask server with Socket.IO on port 8080...")
        socketio.run(app, debug=True, port=8080)

    except FileNotFoundError as e:
        print(f"FATAL ERROR: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()