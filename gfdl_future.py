import asyncio
import websockets
import json
import requests
import functools
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

# ============================== CONFIGURATION =================================
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Ensure environment variables are present
if not all([API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    print("❌ Error: Missing API_KEY, TELEGRAM_BOT_TOKEN, or TELEGRAM_CHAT_ID in Railway variables.")
    sys.exit(1)

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# Using Continuous Symbols for better OI data reliability
SYMBOLS_TO_MONITOR = ["SBIN-I.NFO", "HDFCBANK-I.NFO", "ICICIBANK-I.NFO", "BANKNIFTY-I.NFO"]

LOT_SIZES = {"BANKNIFTY": 30, "HDFCBANK": 550, "ICICIBANK": 700, "SBIN": 750}

# ============================== STATE & UTILITIES =============================
symbol_data_state = {s: {"price": 0, "oi": 0, "oi_prev": 0} for s in SYMBOLS_TO_MONITOR}

def get_now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_telegram(msg: str):
    """Sends Telegram message using a thread pool to avoid blocking the loop."""
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}
    try:
        # Run blocking request in executor
        await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))
    except Exception as e:
        print(f"⚠️ Telegram Failed: {e}", flush=True)

# =============================== CORE LOGIC ===================================
async def process_data(data):
    symbol = data.get("InstrumentIdentifier")
    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")

    if not symbol or symbol not in symbol_data_state or new_price is None or new_oi is None:
        return

    state = symbol_data_state[symbol]
    
    # Initialize state if first time
    if state["oi"] == 0:
        state["price"], state["oi"] = new_price, new_oi
        print(f"ℹ️ [{get_now()}] {symbol}: Initialized (Price: {new_price}, OI: {new_oi})", flush=True)
        return

    oi_chg = new_oi - state["oi"]
    if oi_chg != 0:
        # Simple alert logic for Futures
        lot_size = next((v for k, v in LOT_SIZES.items() if k in symbol), 75)
        lots = int(abs(oi_chg) / lot_size)
        
        if lots > 50:
            msg = f"🔔 *ALERT: {symbol}*\nOI Change: {oi_chg} ({lots} lots)\nPrice: {new_price}\nTime: {get_now()}"
            await send_telegram(msg)
            print(f"🚀 Alert sent for {symbol}", flush=True)

    state["price"], state["oi"] = new_price, new_oi

# ============================ MAIN SCANNER LOOP ===============================
async def run_scanner():
    """Main websocket loop with improved reconnection logic."""
    while True:
        try:
            print(f"🔄 [{get_now()}] Connecting to GDFL...", flush=True)
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as ws:
                # 1. Authenticate
                await ws.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                resp = json.loads(await ws.recv())
                
                if not resp.get("Complete"):
                    print(f"❌ Auth Failed: {resp.get('Comment')}", flush=True)
                    await asyncio.sleep(60) # Longer wait on auth failure
                    continue

                # 2. Subscribe
                for s in SYMBOLS_TO_MONITOR:
                    await ws.send(json.dumps({
                        "MessageType": "SubscribeRealtime", "Exchange": "NFO", 
                        "InstrumentIdentifier": s
                    }))
                
                print(f"✅ [{get_now()}] Connected & Subscribed.", flush=True)
                await send_telegram("✅ GFDL Scanner is LIVE.")

                # 3. Listen
                async for message in ws:
                    data = json.loads(message)
                    if data.get("MessageType") == "RealtimeResult":
                        await process_data(data)

        except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
            print(f"⚠️ Connection lost ({e}). Retrying in 30s...", flush=True)
            await asyncio.sleep(30)
        except Exception as e:
            print(f"🔥 Unexpected Error: {e}", flush=True)
            await asyncio.sleep(10)

async def main():
    """Unified entry point to prevent loop closure errors."""
    try:
        await run_scanner()
    except Exception as e:
        print(f"💥 CRITICAL SHUTDOWN: {e}", flush=True)
        # Attempt one final alert before loop ends
        try:
            await send_telegram(f"💥 Scanner Crashed: {e}")
        except:
            pass

if __name__ == "__main__":
    asyncio.run(main())
