

import asyncio
import websockets
import json
import requests
import functools
import os
import sys
import ssl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ============================== CONFIGURATION =================================
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# Using Continuous Format with .NFO suffix as required
SYMBOLS_TO_MONITOR = ["AXISBANK-I", "KOTAKBANK-I", "RELIANCE-I"]
LOT_SIZES = {"AXISBANK": 625, "KOTAKBANK": 2000, "RELIANCE": 500}


# ============================== STATE & UTILITIES =============================
symbol_data_state = {
    s: {
        "last_price": 0, "last_oi": 0, # Keep track of last tick values
        "open_minute_price": 0, "open_minute_oi": 0,
        "current_minute_high": 0, "current_minute_low": float('inf'), # Initialize low to infinity
        "current_minute_end_price": 0, "current_minute_end_oi": 0,
        "last_processed_minute": None # Datetime object for minute detection
    } for s in SYMBOLS_TO_MONITOR
}

def get_now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_telegram(msg: str):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}
    try:
        await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))
    except Exception as e:
        print(f"⚠️ Telegram Log: {e}", flush=True)

# =============================== CORE LOGIC ===================================
async def process_data(data):
    # print(data) # Keep for debugging if needed, but can be noisy
    symbol = data.get("InstrumentIdentifier")
    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")
    server_time_unix = data.get("ServerTime")

    if not symbol or symbol not in symbol_data_state or new_price is None or new_oi is None or server_time_unix is None:
        return

    state = symbol_data_state[symbol]
    
    # Convert server_time_unix to a datetime object in 'Asia/Kolkata' timezone
    kst_timezone = ZoneInfo("Asia/Kolkata")
    current_tick_time = datetime.fromtimestamp(server_time_unix, kst_timezone)
    current_minute_start = current_tick_time.replace(second=0, microsecond=0)

    # --- Initialize state for the first tick ever for this symbol ---
    if state["last_processed_minute"] is None:
        state["last_price"] = new_price
        state["last_oi"] = new_oi
        state["open_minute_price"] = new_price
        state["open_minute_oi"] = new_oi
        state["current_minute_high"] = new_price
        state["current_minute_low"] = new_price
        state["current_minute_end_price"] = new_price
        state["current_minute_end_oi"] = new_oi
        state["last_processed_minute"] = current_minute_start
        print(f"🟢 [{get_now()}] {symbol}: Initial Minute Data Set (P: {new_price}, OI: {new_oi})", flush=True)
        return

    # --- Minute Rollover Detection ---
    if current_minute_start > state["last_processed_minute"]:
        # A new minute has started, process the data from the just-ended minute
        # We need to pass the *previous* minute's aggregated data for alerting
        await _process_minute_data_and_alert(symbol, state, state["last_processed_minute"])

        # Reset state for the new minute
        state["open_minute_price"] = new_price
        state["open_minute_oi"] = new_oi
        state["current_minute_high"] = new_price
        state["current_minute_low"] = new_price
        state["current_minute_end_price"] = new_price
        state["current_minute_end_oi"] = new_oi
        state["last_processed_minute"] = current_minute_start
    else:
        # Still within the same minute, aggregate data
        state["current_minute_high"] = max(state["current_minute_high"], new_price)
        state["current_minute_low"] = min(state["current_minute_low"], new_price)
        state["current_minute_end_price"] = new_price
        state["current_minute_end_oi"] = new_oi
    
    # Update last tick values for next tick-by-tick comparison if needed (though not used for minute aggregation)
    state["last_price"] = new_price
    state["last_oi"] = new_oi

async def _process_minute_data_and_alert(symbol, state, minute_start_time):
    """
    Calculates 1-minute metrics and triggers alerts based on the aggregated data
    from the minute that just ended.
    """
    # Ensure there's data for the minute to process
    if state["open_minute_oi"] == 0 and state["current_minute_end_oi"] == 0:
        return # No OI data for the minute

    # --- Calculate 1-minute OI metrics ---
    oi_chg_1min = state["current_minute_end_oi"] - state["open_minute_oi"]
    oi_roc_1min = 0.0
    if state["open_minute_oi"] != 0:
        oi_roc_1min = (oi_chg_1min / state["open_minute_oi"]) * 100

    # --- Calculate 1-minute Price metrics ---
    price_chg_1min = state["current_minute_end_price"] - state["open_minute_price"]
    price_chg_percent_1min = 0.0
    if state["open_minute_price"] != 0:
        price_chg_percent_1min = (price_chg_1min / state["open_minute_price"]) * 100

    # --- Calculate Lots for the minute ---
    base_symbol = symbol.split("-")[0]
    lot_size = LOT_SIZES.get(base_symbol, 75)
    lots_1min = int(abs(oi_chg_1min) / lot_size)

    # --- Alert Conditions based on 1-minute data ---
    # Example: If OI changed by more than 1 lot AND Price changed by more than 0.1%
    if lots_1min > 1 and abs(price_chg_percent_1min) >= 0.1: # Example combined condition
        direction_arrow = "🔺" if price_chg_1min > 0 else "🔻"
        
        # Format the alert message with 1-minute aggregated data
        alert_msg = (f"⏱️ 1-Min ALERT: {symbol} {direction_arrow}\n"
                     f"Minute: {minute_start_time.strftime('%H:%M')} - {(minute_start_time + timedelta(minutes=1)).strftime('%H:%M')}\n"
                     f"OI Change (1 min): {oi_chg_1min} ({lots_1min} lots)\n"
                     f"OI RoC (1 min): {oi_roc_1min:.2f}%\n"
                     f"Price Change (1 min): {price_chg_percent_1min:.2f}% (Open: {state['open_minute_price']:.2f}, Close: {state['current_minute_end_price']:.2f})\n"
                     f"Minute High: {state['current_minute_high']:.2f}, Low: {state['current_minute_low']:.2f}\n"
                     f"Time: {get_now()}")
        
        await send_telegram(alert_msg)
        print(f"🚀 Alert: {symbol} 1-minute aggregated alert triggered.", flush=True)

# ... (rest of the script)

# ============================ MAIN SCANNER LOOP ===============================
async def run_scanner():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while True:
        try:
            print(f"🔄 [{get_now()}] Connecting...", flush=True)
            async with websockets.connect(WSS_URL, ssl=ssl_context, ping_interval=20, ping_timeout=20) as ws:
                # Authenticate
                await ws.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                auth_resp = json.loads(await ws.recv())
                
                if not auth_resp.get("Complete"):
                    print(f"❌ Auth Failed: {auth_resp.get('Comment')}", flush=True)
                    await asyncio.sleep(60)
                    continue

                print(f"✅ [{get_now()}] Auth Success. Subscribing to {SYMBOLS_TO_MONITOR}...", flush=True)
                for s in SYMBOLS_TO_MONITOR:
                    await ws.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "InstrumentIdentifier": s}))
                
                await send_telegram("✅ GFDL Scanner is ACTIVE and waiting for first trades.")

                async for message in ws:
                    data = json.loads(message)
                    if data.get("MessageType") == "RealtimeResult":
                        await process_data(data)

        except Exception as e:
            print(f"⚠️ Connection Error: {e}. Retrying in 30s...", flush=True)
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(run_scanner())
