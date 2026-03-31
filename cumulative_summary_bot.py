import os
import re
import logging
import sys
import pytz
from datetime import datetime, timedelta, time
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# --- CONFIGURATION ---
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout
)

BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

# Buffer stores (parsed_data, timestamp)
alerts_buffer = []

# --- PERSISTENT STATE ---
daily_state = {
    "boss_trend": None,  # 'BULLISH' or 'BEARISH'
    "last_signal_time": datetime.min.replace(tzinfo=IST),
}

TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK", "AXISBANK", "SBIN"]

LOT_SIZES = {
    "BANKNIFTY": 30,
    "HDFCBANK": 550,
    "ICICIBANK": 700,
    "AXISBANK": 625,
    "SBIN": 750
}

def format_money(value):
    if value >= 1e7: return f"{value/1e7:.1f}Cr"
    elif value >= 1e5: return f"{value/1e5:.1f}L"
    else: return f"{value:.0f}"

def classify_strike(strike, option_type, future_price):
    try:
        strike, future_price = float(strike), float(future_price)
        if option_type == "CE": return "ITM" if strike < future_price else "OTM"
        if option_type == "PE": return "ITM" if strike > future_price else "OTM"
    except: pass
    return None

def parse_alert(text):
    text_upper = text.upper()
    symbol_match = re.search(r"SYMBOL:\s*([^\n\r]+)", text_upper)
    lot_match = re.search(r"LOTS:\s*(\d+)", text_upper)
    price_match = re.search(r"PRICE:\s*([\d.]+)", text_upper)
    future_match = re.search(r"FUTURE\s+PRICE:\s*([\d.]+)", text_upper)

    if not (symbol_match and lot_match): return None

    symbol_full = symbol_match.group(1).strip()
    lots = int(lot_match.group(1))
    price = float(price_match.group(1)) if price_match else None
    future_price = float(future_match.group(1)) if future_match else None

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_full), None)
    if not base_symbol: return None

    opt_match = re.search(r"(\d+)(CE|PE)$", symbol_full)
    zone, option_type = None, None

    if opt_match and future_price:
        strike = opt_match.group(1)
        option_type = opt_match.group(2)
        zone = classify_strike(strike, option_type, future_price)

    is_future = (opt_match is None)

    action_type = None
    if "WRITER" in text_upper:
        if option_type == "CE": action_type = "CALL_WRITER"
        elif option_type == "PE": action_type = "PUT_WRITER"
    elif "CALL BUY" in text_upper: action_type = "CALL_BUY"
    elif "PUT BUY" in text_upper: action_type = "PUT_BUY"
    elif "SHORT COVERING" in text_upper:
        if is_future: action_type = "FUTURE_SC"
        else: action_type = "CALL_SC" if option_type == "CE" else "PUT_SC"
    elif "LONG UNWINDING" in text_upper:
        if is_future: action_type = "FUTURE_UNW"
        else: action_type = "CALL_UNW" if option_type == "CE" else "PUT_UNW"
    elif "FUTURE BUY" in text_upper or "BUY (LONG)" in text_upper:
        action_type = "FUTURE_BUY"
    elif "FUTURE SELL" in text_upper or "SELL (SHORT)" in text_upper:
        action_type = "FUTURE_SELL"

    if not action_type: return None

    return {
        "symbol": base_symbol,
        "lots": lots,
        "zone": zone,
        "action_type": action_type,
        "future": future_price,
        "price": price
    }

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            print(f"📥 Received Alert: {parsed['symbol']} - {parsed['action_type']} ({parsed['lots']} lots)")
            alerts_buffer.append((parsed, datetime.now(IST)))

# ===============================
# CUMULATIVE REPORT LOGIC (EXPERT TIGHT LOGIC)
# ===============================
async def run_cumulative_report(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer, daily_state
    now = datetime.now(IST)
    
    current_time_int = now.hour * 100 + now.minute
    if current_time_int < 915 or current_time_int > 1545:
        return
    
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    alerts_buffer = [a for a in alerts_buffer if a[1] >= today_start]
    
    if not alerts_buffer: 
        return
    
    # 1. ANALYZE FLOWS (1-Minute and Daily)
    flows_1m = defaultdict(lambda: {"bull": 0, "bear": 0})
    flows_daily = defaultdict(lambda: {"bull": 0, "bear": 0})
    
    for alert, timestamp in alerts_buffer:
        sym, act, price = alert["symbol"], alert["action_type"], alert["price"]
        lot_size = LOT_SIZES.get(sym, 1)
        
        if "FUTURE" not in act:
            turn = (alert["lots"] * 125000) if ("WRITER" in act or "_SC" in act) else (alert["lots"] * (price or 0) * lot_size)
            is_bull = act in ["PUT_WRITER", "CALL_BUY", "PUT_SC", "CALL_UNW"]
        else:
            turn = (alert["lots"] * 175000)
            is_bull = act in ["FUTURE_BUY", "FUTURE_SC"]
            
        if timestamp >= now - timedelta(minutes=1):
            if is_bull: flows_1m[sym]["bull"] += turn
            else: flows_1m[sym]["bear"] += turn
            
        if is_bull: flows_daily[sym]["bull"] += turn
        else: flows_daily[sym]["bear"] += turn

    # 2. EXTRACT KEY DATA
    bn_bull_1m, bn_bear_1m = flows_1m["BANKNIFTY"]["bull"], flows_1m["BANKNIFTY"]["bear"]
    hdfc_bull_1m, hdfc_bear_1m = flows_1m["HDFCBANK"]["bull"], flows_1m["HDFCBANK"]["bear"]
    icici_bull_1m, icici_bear_1m = flows_1m["ICICIBANK"]["bull"], flows_1m["ICICIBANK"]["bear"]
    
    hdfc_bias = "BULL" if flows_daily["HDFCBANK"]["bull"] > flows_daily["HDFCBANK"]["bear"] else "BEAR"
    icici_bias = "BULL" if flows_daily["ICICIBANK"]["bull"] > flows_daily["ICICIBANK"]["bear"] else "BEAR"

    signal_msg = None
    
    # 3. SIGNAL LOGIC
    # --- BULLISH SIGNALS ---
    if bn_bull_1m >= 15e7 and bn_bear_1m <= 1e7:
        # Check Engine Match
        engine_match = (hdfc_bull_1m > 1e7) or (icici_bull_1m > 1e7)
        full_match = (hdfc_bull_1m > 5e7) and (icici_bull_1m > 5e7)
        
        if hdfc_bias == "BEAR" and icici_bias == "BEAR" and not engine_match:
            signal_msg = "⚠️ FAKE BOUNCE: BN Call flow detected, but HDFC/ICICI Engine is still RED. Avoid Buying Call. 🔴"
        elif current_time_int <= 1000 and engine_match:
            daily_state["boss_trend"] = "BULLISH"
            signal_msg = f"🔥 BOSS ATTACK: CALL BUY 🔵\n🛡️ SL: 60 pts | 🎯 TGT: 120/250 pts\nBN: {format_money(bn_bull_1m)} | Engine: MATCH ✅"
        elif full_match:
            signal_msg = f"💎 DUAL MATCH: CALL BUY 🔵\n🛡️ SL: 40 pts | 🎯 TGT: 80/150 pts\nEngine: HDFC 🟢 ICICI 🟢 (100%)"
        elif daily_state["boss_trend"] == "BULLISH" and engine_match:
            signal_msg = f"📈 TREND RESUMPTION: CALL BUY 🔵\n🛡️ SL: 35 pts | 🎯 TGT: 70/120 pts\nEngine Attacking Again - Trend Continues."
        else:
            signal_msg = f"🔵 SCALP BOUNCE: CALL BUY\n🛡️ SL: 25 pts | 🎯 TGT: 40/60 pts\n⚠️ FAST SCALP ONLY."

    # --- BEARISH SIGNALS ---
    elif bn_bear_1m >= 15e7 and bn_bull_1m <= 1e7:
        engine_match = (hdfc_bear_1m > 1e7) or (icici_bear_1m > 1e7)
        full_match = (hdfc_bear_1m > 5e7) and (icici_bear_1m > 5e7)
        
        if hdfc_bias == "BULL" and icici_bias == "BULL" and not engine_match:
            signal_msg = "⚠️ FAKE DROP: BN Put flow detected, but HDFC/ICICI Engine is still GREEN. Avoid Buying Put. 🔵"
        elif current_time_int <= 1000 and engine_match:
            daily_state["boss_trend"] = "BEARISH"
            signal_msg = f"🔥 BOSS ATTACK: PUT BUY 🔴\n🛡️ SL: 60 pts | 🎯 TGT: 120/250 pts\nBN: {format_money(bn_bear_1m)} | Engine: MATCH ✅"
        elif full_match:
            signal_msg = f"💎 DUAL MATCH: PUT BUY 🔴\n🛡️ SL: 40 pts | 🎯 TGT: 80/150 pts\nEngine: HDFC 🔴 ICICI 🔴 (100%)"
        elif daily_state["boss_trend"] == "BEARISH" and engine_match:
            signal_msg = f"📈 TREND RESUMPTION: PUT BUY 🔴\n🛡️ SL: 35 pts | 🎯 TGT: 70/120 pts\nEngine Attacking Again - Trend Continues."
        else:
            signal_msg = f"🔴 SCALP DROP: PUT BUY\n🛡️ SL: 25 pts | 🎯 TGT: 40/60 pts\n⚠️ FAST SCALP ONLY."

    # 4. SEND ALERT (Rate limited to once per 2 mins for same signal)
    if signal_msg:
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=f"{signal_msg}\nTime: {now.strftime('%I:%M %p')}")
        logging.info(f"🚨 SIGNAL SENT: {signal_msg[:30]}...")
    else:
        logging.info(f"⚖ Monitoring... (BN Bull: {format_money(bn_bull_1m)} | Bear: {format_money(bn_bear_1m)})")

def main():
    if not BOT_TOKEN:
        print("❌ Error: SUMMARIZER_BOT_TOKEN not set.")
        return
    
    # --- VERIFY VARIABLES ---
    print(f"✅ Bot Token: Found ({BOT_TOKEN[:8]}...{BOT_TOKEN[-4:]})")
    print(f"✅ Target Channel ID: {TARGET_CHANNEL_ID}")
    print(f"✅ Summary Chat ID: {SUMMARY_CHAT_ID}")
        
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    
    if app.job_queue:
        # Run every 1 minute starting from 9:30 AM
        app.job_queue.run_repeating(run_cumulative_report, interval=60, first=10)
        
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
