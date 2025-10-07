import json
import os
import traceback
import time
from flask import Blueprint, render_template, request
from flask_socketio import emit
from threading import Thread
from datetime import datetime

class BubbleChartLogic:
    def __init__(self, socketio):
        self.socketio = socketio
        self.bp = Blueprint('bubble_chart', __name__, template_folder='templates')
        self.clients = {}
        self.ohlc_data = {}
        self.trades_by_exact_time = {}
        self.last_trade_price = {}
        self.file_data_cache = {}
        self.available_securities = []
        self.HISTORY_FILE = f"static/UpstoxWSS_{datetime.now().strftime('%d_%m_%y')}.txt"
        self.CURRENT_INTERVAL_MS = 60000
        self.THRESHOLD_QTY = 20
        self.BIG_PLAYER_THRESHOLD = 50
        self.GLOBAL_AVG_LTQ = 50

        # Start loading the file in a background thread. This is non-blocking.
        self.data_loading_thread = Thread(target=self._load_file_data)
        self.data_loading_thread.daemon = True
        self.data_loading_thread.start()

        self._register_routes_and_handlers()

    def _register_routes_and_handlers(self):
        namespace = '/bubble'
        @self.bp.route('/BubbleChart')
        def bubble_chart_page():
            symbol = request.args.get('Symbol', 'DEFAULT_SYMBOL')
            return render_template('BubbleChart.html', symbol=symbol)

        @self.socketio.on('connect', namespace=namespace)
        def handle_connect():
            self.clients[request.sid] = {'symbol': None}
            # Asynchronously send securities list once data is loaded.
            self.socketio.start_background_task(self._send_available_securities, request.sid)

        @self.socketio.on('request_data', namespace=namespace)
        def handle_data_request(req):
            symbol, sid = req.get('symbol'), request.sid
            if not symbol: return
            self.clients[sid]['symbol'] = symbol
            self.CURRENT_INTERVAL_MS = self._get_interval_ms(req.get('interval', '1T'))
            self.socketio.join_room(symbol, sid=sid, namespace=namespace)
            if not req.get('loaded', False):
                # This task will wait for the file load to finish internally.
                self.socketio.start_background_task(self._send_aggregated_historical_data, symbol, sid)

        @self.socketio.on('replay_history', namespace=namespace)
        def handle_replay_request(req):
            symbol, sid = req.get('symbol'), request.sid
            if symbol and sid:
                self.socketio.start_background_task(self._run_history_replay, symbol, sid)

    def _send_available_securities(self, sid):
        # Correctly wait for data loading in this separate background task.
        self.data_loading_thread.join()
        self.socketio.emit('available_securities', {'securities': self.available_securities}, room=sid, namespace='/bubble')

    def _load_file_data(self):
        start_time = time.time()
        if not os.path.exists(self.HISTORY_FILE):
            return
        try:
            with open(self.HISTORY_FILE, 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line.strip())
                        if data.get('feeds'):
                            for sec_id, feed_data in data['feeds'].items():
                                if 'ltpc' in feed_data:
                                    if sec_id not in self.file_data_cache:
                                        self.file_data_cache[sec_id] = []
                                    self.file_data_cache[sec_id].append(feed_data['ltpc'])
                    except (json.JSONDecodeError, TypeError):
                        continue
            self.available_securities = sorted(self.file_data_cache.keys())
            end_time = time.time()
            print(f"--> BG_LOAD: File loaded in {end_time - start_time:.2f}s.")
        except Exception as e:
            traceback.print_exc()

    def _send_aggregated_historical_data(self, security_id, sid):
        # Correctly wait for data loading before aggregation.
        self.data_loading_thread.join()
        try:
            aggregated_data = self._aggregate_historical_data(security_id)
            self.socketio.emit('historical_data', {'securityId': security_id, 'data': aggregated_data}, room=sid, namespace='/bubble')
        except Exception as e:
            traceback.print_exc()

    def _run_history_replay(self, security_id, sid):
        self.data_loading_thread.join() # Ensure data is loaded before replay
        self._reset_aggregation(security_id)
        if security_id not in self.file_data_cache: return
        for ltpc_data in self.file_data_cache[security_id]:
            last_candle, bubbles = self._process_single_trade(security_id, {'ltpc': ltpc_data})
            if last_candle:
                self.socketio.emit('live_update', {'securityId': security_id, 'lastCandle': last_candle, 'bubbles': bubbles}, room=sid, namespace='/bubble')
                self.socketio.sleep(0.050)

    def broadcast_live_tick(self, message):
        if hasattr(message, 'feeds') and isinstance(message.feeds, dict):
            for security_id, data in message.feeds.items():
                if security_id in self.ohlc_data:
                    feed = data.to_dict()
                    if 'ltpc' in feed:
                        last_candle, bubbles = self._process_single_trade(security_id, {'ltpc': feed['ltpc']})
                        if last_candle:
                            self.socketio.emit('live_update', {'securityId': security_id, 'lastCandle': last_candle, 'bubbles': bubbles}, room=security_id, namespace='/bubble')

    def _cleanup_client(self, sid):
        if sid in self.clients:
            symbol = self.clients[sid].get('symbol')
            if symbol: self.socketio.leave_room(symbol, sid=sid, namespace='/bubble')
            del self.clients[sid]

    def _reset_aggregation(self, security_id):
        self.ohlc_data[security_id] = {'ohlc': [], 'bubble': []}
        self.trades_by_exact_time[security_id] = {}
        self.last_trade_price.pop(security_id, None)

    def _aggregate_historical_data(self, security_id):
        self._reset_aggregation(security_id)
        if security_id not in self.file_data_cache: return {}
        for ltpc_data in self.file_data_cache[security_id]:
            self._process_single_trade(security_id, {'ltpc': ltpc_data})
        candles = self.ohlc_data[security_id]['ohlc']
        return {'ohlc': [[c['time'], c['open'], c['close'], c['low'], c['high']] for c in candles], 'normalBuy': [[c['time'], c['buyVolume']] for c in candles], 'bigBuy': [[c['time'], c['bigPlayerBuyVolume']] for c in candles], 'normalSell': [[c['time'], -c['sellVolume']] for c in candles], 'bigSell': [[c['time'], -c['bigPlayerSellVolume']] for c in candles], 'bubble': self.ohlc_data[security_id]['bubble']}

    def _process_single_trade(self, security_id, feed):
        if not (feed and feed.get('ltpc') and security_id in self.ohlc_data): return None, None
        ltpc = feed['ltpc']
        time_ms, new_price, quantity = int(ltpc.get('ltt', 0)), float(ltpc.get('ltp', 0)), int(ltpc.get('ltq', 0))
        if not all([time_ms, new_price]): return None, None
        current_ohlc_list = self.ohlc_data[security_id]['ohlc']
        current_bubble_list = self.ohlc_data[security_id]['bubble']
        aggressor = 'HOLD'
        if security_id in self.last_trade_price:
            if new_price > self.last_trade_price[security_id]: aggressor = 'BUY'
            elif new_price < self.last_trade_price[security_id]: aggressor = 'SELL'
        if time_ms not in self.trades_by_exact_time.get(security_id, {}):
            self.trades_by_exact_time.setdefault(security_id, {})[time_ms] = {'trades': [], 'price': new_price}
        self.trades_by_exact_time[security_id][time_ms]['trades'].append({'quantity': quantity})
        current_group = self.trades_by_exact_time[security_id][time_ms]
        sum_ltq = sum(t['quantity'] for t in current_group['trades'])
        if sum_ltq >= self.THRESHOLD_QTY:
            if not any(b['x'] == time_ms for b in current_bubble_list):
                current_bubble_list.append({'x': time_ms, 'y': new_price, 'q': sum_ltq, 'impactScore': max(t['quantity'] for t in current_group['trades']) / self.GLOBAL_AVG_LTQ})
        interval_start_ms = (time_ms // self.CURRENT_INTERVAL_MS) * self.CURRENT_INTERVAL_MS
        current_bucket = current_ohlc_list[-1] if current_ohlc_list else None
        if not current_bucket or current_bucket['time'] != interval_start_ms:
            current_bucket = self._get_empty_bucket(interval_start_ms, new_price)
            current_ohlc_list.append(current_bucket)
        current_bucket.update({'close': new_price, 'high': max(current_bucket['high'], new_price), 'low': min(current_bucket['low'], new_price)})
        is_big = quantity >= self.BIG_PLAYER_THRESHOLD
        if aggressor in ('BUY', 'HOLD'):
            current_bucket['bigPlayerBuyVolume' if is_big else 'buyVolume'] += quantity
        else:
            current_bucket['bigPlayerSellVolume' if is_big else 'sellVolume'] += quantity
        self.last_trade_price[security_id] = new_price
        return current_bucket, current_bubble_list

    def _get_interval_ms(self, interval_str):
        if not interval_str: return 60000
        if interval_str.endswith('S'): return int(interval_str[:-1]) * 1000
        if interval_str.endswith('T'): return int(interval_str[:-1]) * 60000
        return 60000

    def _get_empty_bucket(self, time_ms, price):
        return {'time': time_ms, 'open': price, 'high': price, 'low': price, 'close': price, 'buyVolume': 0, 'bigPlayerBuyVolume': 0, 'sellVolume': 0, 'bigPlayerSellVolume': 0}