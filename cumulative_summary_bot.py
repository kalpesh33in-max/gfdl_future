import os
import re
import logging
import sys
import pytz
from datetime import datetime, timedelta
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
    "last_signal_text": ""
}

TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK", "AXISBANK", "SBIN"]

# Updated Lot Sizes based on user corrections
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
        action_type = "CALL_WRITER" if option_type == "CE" else "PUT_WRITER"
    elif "CALL BUY" in text_upper: action_type = "CALL_BUY"
    elif "PUT BUY" in text_upper: action_type = "PUT_BUY"
    elif "SHORT COVERING" in text_upper:
        if is_future: action_type = "FUTURE_SC"
        else: action_type = "CALL_SC" if option_type == "CE" else "PUT_SC"
    elif "LONG UNWINDING" in text_upper:
        if is_future: action_type = "FUTURE_UNW"
        else: action_type = "CALL_UNW" if option_type == "CE" else "PUT_UNW"
    elif any(x in text_upper for x in ["FUTURE BUY", "BUY (LONG)"]): action_type = "FUTURE_BUY"
    elif any(x in text_upper for x in ["FUTURE SELL", "SELL (SHORT)"]): action_type = "FUTURE_SELL"

    if not action_type: return None

    return {
        "symbol": base_symbol,
        "lots": lots,
        "zone": zone,
        "action_type": action_type,
        "future": future_price,
        "price": price
    }

async def run_cumulative_report(context: ContextTypes.DEFAULT_TYPE):
    """
    Analyzes the buffer using a sliding 120-second window.
    Called immediately upon receiving any new message.
    """
    global alerts_buffer, daily_state
    now = datetime.now(IST)
    
    current_time_int = now.hour * 100 + now.minute
    if current_time_int < 915 or current_time_int > 1545:
        return
    
    # Dynamic Window: 120 Seconds
    window_start = now - timedelta(seconds=120)
    recent_alerts = [a for a in alerts_buffer if a[1] >= window_start]
    
    if not recent_alerts: return
    
    flows_120s = defaultdict(lambda: {"bull": 0, "bear": 0})
    flows_daily = defaultdict(lambda: {"bull": 0, "bear": 0})
    
    # Calculate weights
    for alert, timestamp in alerts_buffer:
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if timestamp < today_start: continue

        sym, act, price = alert["symbol"], alert["action_type"], alert["price"]
        lot_size = LOT_SIZES.get(sym, 1)
        
        if "FUTURE" not in act:
            # Fixed 1.25L weight for writers/SC as per your preference
            turn = (alert["lots"] * 125000) if ("WRITER" in act or "_SC" in act) else (alert["lots"] * (price or 0) * lot_size)
            is_bull = act in ["PUT_WRITER", "CALL_BUY", "PUT_SC", "CALL_UNW"]
        else:
            # Fixed 1.75L weight for futures
            turn = (alert["lots"] * 175000)
            is_bull = act in ["FUTURE_BUY", "FUTURE_SC"]
            
        if timestamp >= window_start:
            if is_bull: flows_120s[sym]["bull"] += turn
            else: flows_120s[sym]["bear"] += turn
            
        if is_bull: flows_daily[sym]["bull"] += turn
        else: flows_daily[sym]["bear"] += turn

    # Data Extraction
    bn_bull, bn_bear = flows_120s["BANKNIFTY"]["bull"], flows_120s["BANKNIFTY"]["bear"]
    hdfc_bull, hdfc_bear = flows_120s["HDFCBANK"]["bull"], flows_120s["HDFCBANK"]["bear"]
    icici_bull, icici_bear = flows_120s["ICICIBANK"]["bull"], flows_120s["ICICIBANK"]["bear"]
    
    hdfc_bias = "BULL" if flows_daily["HDFCBANK"]["bull"] > flows_daily["HDFCBANK"]["bear"] else "BEAR"
    icici_bias = "BULL" if flows_daily["ICICIBANK"]["bull"] > flows_daily["ICICIBANK"]["bear"] else "BEAR"

    signal_msg = None
    engine_match_bull = (hdfc_bull > 1e7) or (icici_bull > 1e7)
    engine_match_bear = (hdfc_bear > 1e7) or (icici_bear > 1e7)
    
    # Signal Logic
    if bn_bull >= 15e7 and bn_bear <= 1e7:
        if hdfc_bias == "BEAR" and icici_bias == "BEAR" and not engine_match_bull:
            signal_msg = "⚠️ FAKE BOUNCE: BN High Flow, Engines RED. Avoid Buying Call. 🔴"
        elif current_time_int <= 1000 and engine_match_bull:
            daily_state["boss_trend"] = "BULLISH"
            signal_msg = f"🔥 BOSS ATTACK: CALL BUY 🔵\nBN: {format_money(bn_bull)} | Engine: MATCH ✅"
        elif hdfc_bull > 5e7 and icici_bull > 5e7:
            signal_msg = f"💎 DUAL MATCH: CALL BUY 🔵\nEngine: HDFC 🟢 ICICI 🟢 (100%)"
        else:
            signal_msg = f"🔵 SCALP BOUNCE: CALL BUY\nBN: {format_money(bn_bull)} | Fast Scalp Only."

    elif bn_bear >= 15e7 and bn_bull <= 1e7:
        if hdfc_bias == "BULL" and icici_bias == "BULL" and not engine_match_bear:
            signal_msg = "⚠️ FAKE DROP: BN High Flow, Engines GREEN. Avoid Buying Put. 🔵"
        elif current_time_int <= 1000 and engine_match_bear:
            daily_state["boss_trend"] = "BEARISH"
            signal_msg = f"🔥 BOSS ATTACK: PUT BUY 🔴\nBN: {format_money(bn_bear)} | Engine: MATCH ✅"
        elif hdfc_bear > 5e7 and icici_bear > 5e7:
            signal_msg = f"💎 DUAL MATCH: PUT BUY 🔴\nEngine: HDFC 🔴 ICICI 🔴 (100%)"
        else:
            signal_msg = f"🔴 SCALP DROP: PUT BUY\nBN: {format_money(bn_bear)} | Fast Scalp Only."

    # Send Message with Rate Limiting (2 mins for identical signals)
    if signal_msg and (signal_msg != daily_state["last_signal_text"] or (now - daily_state["last_signal_time"]).seconds > 120):
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=f"{signal_msg}\nTime: {now.strftime('%I:%M %p')}")
        daily_state["last_signal_time"] = now
        daily_state["last_signal_text"] = signal_msg
        logging.info(f"🚨 SIGNAL SENT: {signal_msg[:30]}")
    else:
        logging.info(f"⚖ Monitoring (120s)... BN Bull: {format_money(bn_bull)} | Bear: {format_money(bn_bear)}")

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            print(f"📥 Received Alert: {parsed['symbol']} - {parsed['action_type']} ({parsed['lots']} lots)")
            alerts_buffer.append((parsed, datetime.now(IST)))
            # Trigger Instant Analysis
            await run_cumulative_report(context)

def main():
    if not BOT_TOKEN:
        print("❌ Error: SUMMARIZER_BOT_TOKEN not set.")
        return
    
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    
    # Job queue acts as a fallback monitor every 60s even if no messages arrive
    if app.job_queue:
        app.job_queue.run_repeating(run_cumulative_report, interval=60, first=10)
        
    print("🚀 Scanner started in Event-Driven Mode (120s Window)...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
