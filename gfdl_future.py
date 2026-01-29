import asyncio
import websockets
import json
import requests
import functools
import os
import sys
import ssl
from datetime import datetime
from zoneinfo import ZoneInfo

# ============================== CONFIGURATION =================================
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# .NFO suffix is mandatory for some GDFL API tiers
SYMBOLS_TO_MONITOR = ["SBIN-I.NFO", "HDFCBANK-I.NFO", "ICICIBANK-I.NFO", "BANKNIFTY-I.NFO"]
LOT_SIZES = {"BANKNIFTY": 30, "HDFCBANK": 550, "ICICIBANK": 700, "SBIN": 750}

# ============================== STATE & UTILITIES =============================
symbol_data_state = {s: {"price": 0, "oi": 0} for s in SYMBOLS_TO_MONITOR}

def get_now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_telegram(msg: str):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}
    try:
        await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))
    except Exception as e:
        print(f"⚠️ Telegram Error: {e}", flush=True)

# =============================== CORE LOGIC ===================================
async def process_data(data):
    symbol = data.get("InstrumentIdentifier")
    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")

    if not symbol or symbol not in symbol_data_state or new_price is None or new_oi is None:
        return

    state = symbol_data_state[symbol]
    
    if state["oi"] == 0:
        state["price"], state["oi"] = new_price, new_oi
        print(f"🟢 [{get_now()}] {symbol}: Initialized (P: {new_price}, OI: {new_oi})", flush=True)
        return

    oi_chg = new_oi - state["oi"]
    if abs(oi_chg) > 0: 
        symbol_key = symbol.replace("-I.NFO", "")
        lot_size = LOT_SIZES.get(symbol_key, 75)
        lots = int(abs(oi_chg) / lot_size)
        
        # Reduced to 1 lot for testing; increase to 50 for production
        if lots >= 1:
            direction = "🔺" if new_price > state["price"] else "🔻"
            msg = f"🔔 *ALERT: {symbol}* {direction}\nOI Change: {oi_chg} ({lots} lots)\nPrice: {new_price}\nTime: {get_now()}"
            await send_telegram(msg)
            print(f"🚀 Alert: {symbol} movement of {lots} lots detected.", flush=True)

    state["price"], state["oi"] = new_price, new_oi

# ============================ MAIN SCANNER LOOP ===============================
async def run_scanner():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while True:
        try:
            print(f"🔄 [{get_now()}] Connecting...", flush=True)
            async with websockets.connect(WSS_URL, ssl=ssl_context, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                auth_resp = json.loads(await ws.recv())
                
                if not auth_resp.get("Complete"):
                    print(f"❌ Auth Failed: {auth_resp.get('Comment')}", flush=True)
                    await asyncio.sleep(60)
                    continue

                print(f"✅ [{get_now()}] Auth Success. Subscribing...", flush=True)
                for s in SYMBOLS_TO_MONITOR:
                    await ws.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "InstrumentIdentifier": s}))
                
                await send_telegram("✅ GFDL Scanner is ACTIVE on Railway.")

                last_heartbeat = datetime.now()
                async for message in ws:
                    # Connection Heartbeat every 5 mins
                    if (datetime.now() - last_heartbeat).seconds > 300:
                        print(f"💓 [{get_now()}] Heartbeat: Connection healthy.", flush=True)
                        last_heartbeat = datetime.now()

                    data = json.loads(message)
                    if data.get("MessageType") == "RealtimeResult":
                        await process_data(data)

        except Exception as e:
            print(f"⚠️ Connection Error: {e}. Retrying in 30s...", flush=True)
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(run_scanner())
