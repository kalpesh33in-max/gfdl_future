
import os
import re
import logging
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# Logging setup
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Environment Variables
BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

alerts_buffer = []

# ===============================
# CONFIGURATION
# ===============================
TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK"]

ATM_RANGE = {
    "BANKNIFTY": 100,
    "HDFCBANK": 5,
    "ICICIBANK": 10,
}

# ===============================
# FORMATTING: INDIAN NUMBER SYSTEM
# ===============================
def format_indian_value(val):
    """Formats numbers into Indian numbering system (Cr/Lakh)"""
    abs_val = abs(val)
    if abs_val >= 10000000:
        return f"{val / 10000000:.2f} Cr"
    elif abs_val >= 100000:
        return f"{val / 100000:.2f} L"
    else:
        return f"{val:,.0f}"

# ===============================
# PRECISION STRIKE CLASSIFICATION
# ===============================
def classify_strike(symbol, strike, option_type, future_price):
    width = ATM_RANGE.get(symbol, 0)
    if abs(strike - future_price) <= width:
        return "ATM"
    if option_type == "CE":
        return "ITM" if strike < (future_price - width) else "OTM"
    if option_type == "PE":
        return "ITM" if strike > (future_price + width) else "OTM"
    return None

# ===============================
# PARSE ALERT (Turnover Logic)
# ===============================
def parse_alert(text):
    text_upper = text.upper()
    
    symbol_match = re.search(r"SYMBOL:\s*([\w-]+)", text_upper)
    lot_match = re.search(r"LOTS:\s*(\d+)", text_upper)
    price_match = re.search(r"PRICE:\s*([\d.]+)", text_upper)
    oi_match = re.search(r"OI\s+CHANGE\s*:\s*([+-]?[\d,]+)", text_upper)
    future_match = re.search(r"FUTURE\s+PRICE:\s*([\d.]+)", text_upper)

    if not (symbol_match and lot_match):
        return None

    symbol_full = symbol_match.group(1)
    lots = int(lot_match.group(1))
    price = float(price_match.group(1)) if price_match else 0
    future_price = float(future_match.group(1)) if future_match else None
    
    # Extract OI Qty for Options Turnover
    oi_str = oi_match.group(1).replace(",", "").replace("+", "") if oi_match else "0"
    oi_qty = abs(int(oi_str))

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_full), None)
    if not base_symbol:
        return None

    # Determine if it's an Option or Future
    opt_match = re.search(r"(\d+)(CE|PE)", symbol_full)
    strike = None
    option_type = None
    zone = None
    turnover = 0

    if opt_match:
        # OPTIONS TURNOVER: OI Qty * Price
        strike = int(opt_match.group(1))
        option_type = opt_match.group(2)
        turnover = oi_qty * price
        if future_price:
            zone = classify_strike(base_symbol, strike, option_type, future_price)
    else:
        # FUTURE TURNOVER: Lots * 100,000
        turnover = lots * 100000

    # Categorization
    action_type = None
    if "CALL WRITER" in text_upper: action_type = "CALL_WRITER"
    elif "PUT WRITER" in text_upper: action_type = "PUT_WRITER"
    elif "CALL BUY" in text_upper: action_type = "CALL_BUY"
    elif "PUT BUY" in text_upper: action_type = "PUT_BUY"
    elif "SHORT COVERING" in text_upper:
        if opt_match: action_type = "CALL_SC" if option_type == "CE" else "PUT_SC"
        else: action_type = "FUTURE_SC"
    elif "LONG UNWINDING" in text_upper:
        if opt_match: action_type = "CALL_UNW" if option_type == "CE" else "PUT_UNW"
        else: action_type = "FUTURE_UNW"
    elif "FUTURE BUY" in text_upper: action_type = "FUTURE_BUY"
    elif "FUTURE SELL" in text_upper: action_type = "FUTURE_SELL"

    if not action_type: return None

    return {
        "symbol": base_symbol,
        "turnover": turnover,
        "action_type": action_type,
        "zone": zone,
        "current_future": future_price
    }

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            alerts_buffer.append(parsed)

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer: return

    current_batch = list(alerts_buffer)
    alerts_buffer.clear()

    # Data structure for turnover
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    last_known_future = {}

    for alert in current_batch:
        sym = alert["symbol"]
        act = alert["action_type"]
        z = alert["zone"]
        t = alert["turnover"]
        if alert["current_future"]:
            last_known_future[sym] = alert["current_future"]

        if z:
            data[sym][act][z] += t
        else:
            data[sym][act]["TOTAL"] += t

    message = "<pre>
💰 2 MIN TURNOVER FLOW

"
    for symbol in TRACK_SYMBOLS:
        if symbol not in data: continue

        f_price = last_known_future.get(symbol, "N/A")
        message += f"{symbol} (FUT: {f_price})
"
        message += "-" * 56 + "
"
        message += f"{'TYPE':14}{'ITM':>10}{'ATM':>10}{'OTM':>10}{'TOT':>10}
"
        message += "-" * 56 + "
"

        actions = ["CALL_WRITER", "PUT_WRITER", "CALL_BUY", "PUT_BUY", "CALL_SC", "PUT_SC", "CALL_UNW", "PUT_UNW"]
        for action in actions:
            itm = format_indian_value(data[symbol][action]["ITM"])
            atm = format_indian_value(data[symbol][action]["ATM"])
            otm = format_indian_value(data[symbol][action]["OTM"])
            total = format_indian_value(data[symbol][action]["ITM"] + data[symbol][action]["ATM"] + data[symbol][action]["OTM"])
            
            label = action.replace("_", " ")
            message += f"{label:14}{itm:>10}{atm:>10}{otm:>10}{total:>10}
"

        fb = format_indian_value(data[symbol]["FUTURE_BUY"]["TOTAL"])
        fs = format_indian_value(data[symbol]["FUTURE_SELL"]["TOTAL"])
        fsc = format_indian_value(data[symbol]["FUTURE_SC"]["TOTAL"])
        funw = format_indian_value(data[symbol]["FUTURE_UNW"]["TOTAL"])

        message += "-" * 56 + "
"
        message += f"{'FUT BUY':14}{fb:>10}
"
        message += f"{'FUT SELL':14}{fs:>10}
"
        message += f"{'FUT SC':14}{fsc:>10}
"
        message += f"{'FUT UNW':14}{funw:>10}

"

    message += "Validity: Next 2 Minutes Only
</pre>"
    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message, parse_mode="HTML")

def main():
    if not BOT_TOKEN:
        print("Error: SUMMARIZER_BOT_TOKEN not set.")
        return
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    if app.job_queue:
        app.job_queue.run_repeating(process_summary, interval=120, first=10)
    print("Turnover Flow Bot starting...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
