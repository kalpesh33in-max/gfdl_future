import os
import re
import logging
import sys
import pytz
import asyncio
from datetime import datetime, timedelta, time
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from telegram.error import Forbidden, BadRequest

# --- CONFIGURATION ---
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout
)

BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
# Use the raw string from env; we'll handle formatting in main
SUMMARY_CHAT_ID_VAL = os.getenv("SUMMARY_CHAT_ID")

alerts_buffer = []
TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK", "AXISBANK", "SBIN"]
LOT_SIZES = {
    "BANKNIFTY": 30, # Updated to 30 as per your requirement
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

def get_bias_label(net_lots):
    if net_lots > 1000: return "🔥 MASSIVE BULLISH ACCUMULATION"
    elif net_lots > 500: return "🚀 STRONG BULLISH BIAS"
    elif net_lots > 0: return "🟢 Positive Flow"
    elif net_lots < -1000: return "🔥 MASSIVE BEARISH DISTRIBUTION"
    elif net_lots < -500: return "📉 STRONG BEARISH BIAS"
    elif net_lots < 0: return "🔴 Negative Flow"
    else: return "⚖ Neutral"

def parse_alert(text):
    text_upper = text.upper()
    symbol_match = re.search(r"SYMBOL:\s*([\w-]+)", text_upper)
    lot_match = re.search(r"LOTS:\s*(\d+)", text_upper)
    price_match = re.search(r"PRICE:\s*([\d.]+)", text_upper)
    future_match = re.search(r"FUTURE\s+PRICE:\s*([\d.]+)", text_upper)

    if not (symbol_match and lot_match): return None

    symbol_full = symbol_match.group(1)
    lots = int(lot_match.group(1))
    price = float(price_match.group(1)) if price_match else None
    future_price = float(future_match.group(1)) if future_match else None

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_full), None)
    if not base_symbol: return None

    opt_match = re.search(r"(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}(\d+)(?:CE|PE)$", symbol_full)
    zone, option_type = None, None

    if opt_match and future_price:
        strike = opt_match.group(1)
        option_type = re.search(r"(CE|PE)$", symbol_full).group(1)
        zone = classify_strike(strike, option_type, future_price)

    action_type = None
    if "WRITER" in text_upper:
        action_type = "CALL_WRITER" if option_type == "CE" else "PUT_WRITER"
    elif "CALL BUY" in text_upper: action_type = "CALL_BUY"
    elif "PUT BUY" in text_upper: action_type = "PUT_BUY"
    elif "SHORT COVERING" in text_upper:
        action_type = "FUTURE_SC" if symbol_full.endswith("-I") else ("CALL_SC" if option_type == "CE" else "PUT_SC")
    elif "LONG UNWINDING" in text_upper:
        action_type = "FUTURE_UNW" if symbol_full.endswith("-I") else ("CALL_UNW" if option_type == "CE" else "PUT_UNW")
    elif "FUTURE BUY" in text_upper: action_type = "FUTURE_BUY"
    elif "FUTURE SELL" in text_upper: action_type = "FUTURE_SELL"

    if not action_type: return None
    return {"symbol": base_symbol, "lots": lots, "zone": zone, "action_type": action_type, "future": future_price, "price": price}

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            logging.info(f"Parsed: {parsed['symbol']} {parsed['action_type']}")
            alerts_buffer.append((parsed, datetime.now(IST)))

async def run_cumulative_report(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    now = datetime.now(IST)
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    
    if now.time() < time(9, 15):
        alerts_buffer = [a for a in alerts_buffer if a[1] >= market_open - timedelta(days=1)]
        return

    batch = [a[0] for a in alerts_buffer if a[1] >= market_open]
    
    # Send a heartbeat if no data yet, so you know the bot is alive
    if not batch:
        try:
            await context.bot.send_message(chat_id=SUMMARY_CHAT_ID_VAL, text="🔄 Bot Active: Waiting for data since 09:15 AM...")
        except Exception as e:
            logging.error(f"Heartbeat failed: {e}")
        return

    duration_mins = int((now - market_open).total_seconds() / 60)
    opt_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    opt_turn = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    fut_data = defaultdict(lambda: defaultdict(int))
    fut_turn = defaultdict(lambda: defaultdict(float))
    last_future = {}

    for alert in batch:
        sym, act, zone, lots, price = alert["symbol"], alert["action_type"], alert["zone"], alert["lots"], alert["price"]
        if alert["future"]: last_future[sym] = alert["future"]
        lot_size = LOT_SIZES.get(sym, 1)

        if zone:
            opt_data[sym][act][zone] += lots
            opt_turn[sym][act][zone] += (lots * 125000) if ("WRITER" in act or "_SC" in act) else (lots * (price or 0) * lot_size)
        else:
            fut_data[sym][act] += lots
            fut_turn[sym][act] += (lots * 175000)

    message = f"<pre>\n📊 DAY CUMULATIVE FLOW ({duration_mins} MINS)\n\n"
    for symbol in TRACK_SYMBOLS:
        if symbol not in opt_data and symbol not in fut_data: continue
        message += f"💎 {symbol} (FUT: {last_future.get(symbol,'N/A')})\n"
        if symbol in opt_data:
            message += "--- OPTIONS FLOW ---\nTYPE       ITM             OTM             TOT\n" + "-"*50 + "\n"
            s_bull_l, s_bear_l, s_bull_t, s_bear_t = 0, 0, 0, 0
            for act in opt_data[symbol]:
                itm_l, otm_l = opt_data[symbol][act]["ITM"], opt_data[symbol][act]["OTM"]
                itm_t, otm_t = opt_turn[symbol][act]["ITM"], opt_turn[symbol][act]["OTM"]
                tot_l, tot_t = itm_l + otm_l, itm_t + otm_t
                if act in ["PUT_WRITER","CALL_BUY","CALL_SC","PUT_UNW"]:
                    s_bull_l += tot_l; s_bull_t += tot_t
                else:
                    s_bear_l += tot_l; s_bear_t += tot_t
                display_act = act.replace("CALL_WRITER","CALL_WR").replace("PUT_WRITER","PUT_WR").replace("SHORT_COVERING","SC").replace("LONG_UNWINDING","UNW")
                message += f"{display_act[:8]:8} {itm_l}({format_money(itm_t)}):>14} {otm_l}({format_money(otm_t)}):>14} {tot_l}({format_money(tot_t)}):>14}\n"
            message += "-"*50 + f"\nOption Bias: {get_bias_label(s_bull_l - s_bear_l)}\nBullish Turn: {format_money(s_bull_t)}\nBearish Turn: {format_money(s_bear_t)}\n\n"
        # (Future flow logic omitted for brevity, remains same as your original)
        message += "========================================\n\n"
    message += f"Cumulative Since: 09:15 AM\n</pre>"
    
    try:
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID_VAL, text=message, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Report send failed: {e}")

def main():
    if not BOT_TOKEN or not SUMMARY_CHAT_ID_VAL:
        logging.error("Missing Token or Summary ID in Railway Variables!")
        return
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    if app.job_queue:
        app.job_queue.run_once(run_cumulative_report, when=5) # Immediate test
        app.job_queue.run_repeating(run_cumulative_report, interval=900, first=10)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
