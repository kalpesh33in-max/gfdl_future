import os
import re
import logging
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# ===============================
# LOGGING
# ===============================
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

alerts_buffer = []

TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK"]

# ===============================
# INDIAN NUMBER FORMAT
# ===============================
def format_indian_value(val):
    abs_val = abs(val)
    if abs_val >= 10000000:
        return f"{val / 10000000:.2f} Cr"
    elif abs_val >= 100000:
        return f"{val / 100000:.2f} L"
    else:
        return f"{val:,.0f}"

# ===============================
# STRICT & SAFE ITM LOGIC
# ===============================
def classify_strike(strike, option_type, future_price):

    try:
        strike = int(float(strike))
        future_price = int(float(future_price))
    except:
        return None

    if option_type == "CE":
        return "ITM" if strike < future_price else "OTM"

    if option_type == "PE":
        return "ITM" if strike > future_price else "OTM"

    return None

# ===============================
# PARSE ALERT
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

    oi_str = oi_match.group(1).replace(",", "").replace("+", "") if oi_match else "0"
    oi_qty = abs(int(oi_str))

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_full), None)
    if not base_symbol:
        return None

    opt_match = re.search(r"(\d+)(CE|PE)", symbol_full)

    turnover = 0
    zone = None
    option_type = None

    if opt_match:
        strike = opt_match.group(1)
        option_type = opt_match.group(2)
        turnover = oi_qty * price

        if future_price:
            zone = classify_strike(strike, option_type, future_price)
    else:
        turnover = lots * 100000  # Futures turnover

    action_type = None

    if "CALL WRITER" in text_upper: action_type = "CALL_WRITER"
    elif "PUT WRITER" in text_upper: action_type = "PUT_WRITER"
    elif "CALL BUY" in text_upper: action_type = "CALL_BUY"
    elif "PUT BUY" in text_upper: action_type = "PUT_BUY"
    elif "SHORT COVERING" in text_upper:
        if opt_match:
            action_type = "CALL_SC" if option_type == "CE" else "PUT_SC"
        else:
            action_type = "FUTURE_SC"
    elif "LONG UNWINDING" in text_upper:
        if opt_match:
            action_type = "CALL_UNW" if option_type == "CE" else "PUT_UNW"
        else:
            action_type = "FUTURE_UNW"
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

# ===============================
# TELEGRAM HANDLER
# ===============================
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    msg = update.channel_post or update.message

    if msg and msg.text and str(msg.chat_id) == str(TARGET_CHANNEL_ID):
        parsed = parse_alert(msg.text)
        if parsed:
            alerts_buffer.append(parsed)

# ===============================
# SUMMARY PROCESS
# ===============================
async def process_summary(context: ContextTypes.DEFAULT_TYPE):

    global alerts_buffer

    if not alerts_buffer:
        return

    batch = list(alerts_buffer)
    alerts_buffer.clear()

    data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    futures_data = defaultdict(lambda: defaultdict(float))
    last_future = {}

    total_bull = 0
    total_bear = 0

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
        message += "-" * 60 + "\n"
        message += f"{'TYPE':14}{'ITM':>12}{'OTM':>12}{'TOT':>12}\n"
        message += "-" * 60 + "\n"

        actions = [
            "CALL_WRITER","PUT_WRITER",
            "CALL_BUY","PUT_BUY",
            "CALL_SC","PUT_SC",
            "CALL_UNW","PUT_UNW"
        ]

        for action in actions:

            itm = data[symbol][action]["ITM"]
            otm = data[symbol][action]["OTM"]
            total = itm + otm

            message += f"{action.replace('_',' '):14}{format_indian_value(itm):>12}{format_indian_value(otm):>12}{format_indian_value(total):>12}\n"

        message += "-" * 60 + "\n"

        fb = futures_data[symbol]["FUTURE_BUY"]
        fs = futures_data[symbol]["FUTURE_SELL"]
        fsc = futures_data[symbol]["FUTURE_SC"]
        funw = futures_data[symbol]["FUTURE_UNW"]

        message += f"{'FUT BUY':14}{format_indian_value(fb):>12}\n"
        message += f"{'FUT SELL':14}{format_indian_value(fs):>12}\n"
        message += f"{'FUT SC':14}{format_indian_value(fsc):>12}\n"
        message += f"{'FUT UNW':14}{format_indian_value(funw):>12}\n\n"

        # Bullish Money
        bull = (
            data[symbol]["PUT_WRITER"]["ITM"] + data[symbol]["PUT_WRITER"]["OTM"] +
            data[symbol]["CALL_BUY"]["ITM"] + data[symbol]["CALL_BUY"]["OTM"] +
            data[symbol]["CALL_SC"]["ITM"] + data[symbol]["CALL_SC"]["OTM"] +
            data[symbol]["PUT_UNW"]["ITM"] + data[symbol]["PUT_UNW"]["OTM"] +
            fb + fsc
        )

        # Bearish Money
        bear = (
            data[symbol]["CALL_WRITER"]["ITM"] + data[symbol]["CALL_WRITER"]["OTM"] +
            data[symbol]["PUT_BUY"]["ITM"] + data[symbol]["PUT_BUY"]["OTM"] +
            data[symbol]["PUT_SC"]["ITM"] + data[symbol]["PUT_SC"]["OTM"] +
            data[symbol]["CALL_UNW"]["ITM"] + data[symbol]["CALL_UNW"]["OTM"] +
            fs + funw
        )

        total_bull += bull
        total_bear += bear

    net_money = total_bull - total_bear
    total_flow = total_bull + total_bear
    dominance = (total_bull / total_flow * 100) if total_flow > 0 else 0

    if net_money > 50000000:
        strength = "🔥 VERY STRONG BULLISH"
    elif net_money > 10000000:
        strength = "🚀 STRONG BULLISH"
    elif net_money > 0:
        strength = "🟢 Mild Bullish"
    elif net_money < -50000000:
        strength = "🔥 VERY STRONG BEARISH"
    elif net_money < -10000000:
        strength = "📉 STRONG BEARISH"
    elif net_money < 0:
        strength = "🔴 Mild Bearish"
    else:
        strength = "⚖️ Balanced"

    message += "=" * 60 + "\n"
    message += "💸 NET DIRECTIONAL MONEY FLOW (All Symbols)\n"
    message += "=" * 60 + "\n\n"
    message += f"Total Bullish Money : {format_indian_value(total_bull)}\n"
    message += f"Total Bearish Money : {format_indian_value(total_bear)}\n"
    message += f"Net Money Flow      : {format_indian_value(net_money)}\n"
    message += f"Bullish Dominance   : {dominance:.1f}%\n\n"
    message += f"Bias                : {strength}\n\n"
    message += "Validity: Next 2 Minutes Only\n"
    message += "</pre>"

    await context.bot.send_message(
        chat_id=SUMMARY_CHAT_ID,
        text=message,
        parse_mode="HTML"
    )

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    if app.job_queue:
        app.job_queue.run_repeating(process_summary, interval=120, first=10)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
