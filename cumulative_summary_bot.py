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
    
    # Improved symbol regex to capture symbols with spaces
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

    # Robust Option Match: Finds the strike price (numbers) immediately before CE or PE
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
# CUMULATIVE REPORT LOGIC
# ===============================
async def run_cumulative_report(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    now = datetime.now(IST)
    
    # DEBUG LOG: See exactly what time the bot sees
    logging.info(f"🕒 Current IST Time: {now.strftime('%H:%M:%S')}")
    
    # STRICT MARKET HOURS CHECK (9:15 AM to 3:45 PM IST)
    current_time_int = now.hour * 100 + now.minute
    if current_time_int < 915 or current_time_int > 1545:
        logging.info("⏳ Market Closed. Skipping Telegram report.")
        return
    
    # DAILY RESET / TODAY ONLY FILTER: 
    # Remove any alerts that are not from the current calendar day (Today)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    alerts_buffer = [a for a in alerts_buffer if a[1] >= today_start]
    
    if not alerts_buffer: 
        logging.info("📝 No alerts collected for today yet. Waiting...")
        return
    
    # Process EVERYTHING currently in the buffer
    batch = [a[0] for a in alerts_buffer]
    
    # RECENT FLOW (Last 1 Minute) for all tracked symbols
    recent_batch = [a[0] for a in alerts_buffer if a[1] >= now - timedelta(minutes=1)]
    
    # Combined Totals (Options + Futures) for ALL 5 Symbols (1-Minute Window)
    r_bull_opt, r_bull_fut = 0, 0
    r_bear_opt, r_bear_fut = 0, 0
    
    for alert in recent_batch:
        sym, act, price = alert["symbol"], alert["action_type"], alert["price"]
        lot_size = LOT_SIZES.get(sym, 1)
        
        if "FUTURE" not in act:
            # Options: Fixed 1.25L for Writers/SC, actual premium for Buys/Unwinding
            turn = (alert["lots"] * 125000) if ("WRITER" in act or "_SC" in act) else (alert["lots"] * (price or 0) * lot_size)
            if act in ["PUT_WRITER", "CALL_BUY", "PUT_SC", "CALL_UNW"]: r_bull_opt += turn
            else: r_bear_opt += turn
        else:
            # Futures: Fixed 1.75L multiplier
            turn = (alert["lots"] * 175000)
            if act in ["FUTURE_BUY", "FUTURE_SC"]: r_bull_fut += turn
            else: r_bear_fut += turn

    r_bull_total = r_bull_opt + r_bull_fut
    r_bear_total = r_bear_opt + r_bear_fut

    # SIGNAL LOGIC & ALERT
    if r_bear_total >= 15e7 and r_bull_total <= 1e7:
        message = (
            f"Recent Bearish: {format_money(r_bear_opt)} option + {format_money(r_bear_fut)} future = {format_money(r_bear_total)}\n"
            f"Recent Bullish: {format_money(r_bull_opt)} option + {format_money(r_bull_fut)} future = {format_money(r_bull_total)}\n"
            f"🚨🚨 REVERSAL SIGNAL: PUT BUY 🔴🚨🚨\n"
            f"Time: {now.strftime('%I:%M %p')}"
        )
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message)
        logging.info(f"🚨 SIGNAL SENT: PUT BUY ({format_money(r_bear_total)})")
    
    elif r_bull_total >= 15e7 and r_bear_total <= 1e7:
        message = (
            f"Recent Bullish: {format_money(r_bull_opt)} option + {format_money(r_bull_fut)} future = {format_money(r_bull_total)}\n"
            f"Recent Bearish: {format_money(r_bear_opt)} option + {format_money(r_bear_fut)} future = {format_money(r_bear_total)}\n"
            f"🚨🚨 REVERSAL SIGNAL: CALL BUY 🔵🚨🚨\n"
            f"Time: {now.strftime('%I:%M %p')}"
        )
        await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message)
        logging.info(f"🚨 SIGNAL SENT: CALL BUY ({format_money(r_bull_total)})")
    
    else:
        # Quiet background logging
        logging.info(f"⚖ Monitoring... (Bull: {format_money(r_bull_total)} | Bear: {format_money(r_bear_total)})")

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
