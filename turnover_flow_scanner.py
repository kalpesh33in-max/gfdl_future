import os
import re
import logging
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

alerts_buffer = []

TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK"]

# -----------------------------
# FORMAT INDIAN VALUE
# -----------------------------
def format_indian_value(val):
    abs_val = abs(val)
    if abs_val >= 10000000:
        return f"{val / 10000000:.2f} Cr"
    elif abs_val >= 100000:
        return f"{val / 100000:.2f} L"
    else:
        return f"{val:,.0f}"

# -----------------------------
# STRIKE CLASSIFICATION
# -----------------------------
def classify_strike(symbol, strike, option_type, future_price):

    # Intrinsic ITM logic
    is_itm = False
    if option_type == "CE":
        is_itm = strike < future_price
    elif option_type == "PE":
        is_itm = strike > future_price

    if not is_itm:
        return "OTM"

    # BANKNIFTY deep/near logic
    if symbol == "BANKNIFTY":
        distance = abs(strike - future_price)
        if distance <= 300:
            return "ITM_NEAR"
        else:
            return "ITM_DEEP"

    return "ITM_NEAR"

# -----------------------------
# PARSE ALERT
# -----------------------------
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

    oi_str = oi_match.group(1).replace(",", "").replace("+", "") if oi_match else "0"
    oi_qty = abs(int(oi_str))

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_full), None)
    if not base_symbol:
        return None

    opt_match = re.search(r"(\d+)(CE|PE)", symbol_full)
    turnover = 0
    zone = None

    if opt_match:
        strike = int(opt_match.group(1))
        option_type = opt_match.group(2)
        turnover = oi_qty * price
        if future_price:
            zone = classify_strike(base_symbol, strike, option_type, future_price)
    else:
        turnover = lots * 100000

    action_type = None
    if "CALL WRITER" in text_upper: action_type = "CALL_WRITER"
    elif "PUT WRITER" in text_upper: action_type = "PUT_WRITER"
    elif "CALL BUY" in text_upper: action_type = "CALL_BUY"
    elif "PUT BUY" in text_upper: action_type = "PUT_BUY"
    elif "SHORT COVERING" in text_upper:
        action_type = "CALL_SC" if opt_match and option_type == "CE" else "PUT_SC" if opt_match else "FUTURE_SC"
    elif "LONG UNWINDING" in text_upper:
        action_type = "CALL_UNW" if opt_match and option_type == "CE" else "PUT_UNW" if opt_match else "FUTURE_UNW"
    elif "FUTURE BUY" in text_upper: action_type = "FUTURE_BUY"
    elif "FUTURE SELL" in text_upper: action_type = "FUTURE_SELL"

    if not action_type:
        return None

    return {
        "symbol": base_symbol,
        "turnover": turnover,
        "zone": zone,
        "action_type": action_type,
        "future": future_price
    }

# -----------------------------
# TELEGRAM HANDLER
# -----------------------------
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            alerts_buffer.append(parsed)

# -----------------------------
# SUMMARY
# -----------------------------
async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        return

    batch = list(alerts_buffer)
    alerts_buffer.clear()

    data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    futures_data = defaultdict(lambda: defaultdict(float))
    last_future = {}

    for alert in batch:
        sym = alert["symbol"]
        act = alert["action_type"]
        zone = alert["zone"]
        val = alert["turnover"]

        if alert["future"]:
            last_future[sym] = alert["future"]

        if zone:
            data[sym][act][zone] += val
        else:
            futures_data[sym][act] += val

    message = "<pre>\n💰 2 MIN TURNOVER FLOW\n\n"

    for symbol in TRACK_SYMBOLS:
        if symbol not in data and symbol not in futures_data:
            continue

        message += f"{symbol} (FUT: {last_future.get(symbol,'N/A')})\n"
        message += "-" * 65 + "\n"
        message += f"{'TYPE':14}{'ITM_NEAR':>12}{'ITM_DEEP':>12}{'OTM':>12}{'TOT':>12}\n"
        message += "-" * 65 + "\n"

        actions = ["CALL_WRITER","PUT_WRITER","CALL_BUY","PUT_BUY"]

        for action in actions:
            near = data[symbol][action]["ITM_NEAR"]
            deep = data[symbol][action]["ITM_DEEP"]
            otm = data[symbol][action]["OTM"]
            total = near + deep + otm

            message += f"{action.replace('_',' '):14}{format_indian_value(near):>12}{format_indian_value(deep):>12}{format_indian_value(otm):>12}{format_indian_value(total):>12}\n"

        message += "\n"

    message += "Validity: Next 2 Minutes Only\n</pre>"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message, parse_mode="HTML")

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    if app.job_queue:
        app.job_queue.run_repeating(process_summary, interval=120, first=10)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
