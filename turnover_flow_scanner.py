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

TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK", "AXISBANK", "SBIN"]

LOT_SIZES = {
    "BANKNIFTY": 30,
    "HDFCBANK": 550,
    "ICICIBANK": 700,
    "AXISBANK": 625,
    "SBIN": 750
}

# ===============================
# MONEY FORMAT
# ===============================
def format_money(value):
    if value >= 1e7:
        return f"{value/1e7:.2f}Cr"
    elif value >= 1e5:
        return f"{value/1e5:.2f}L"
    else:
        return f"{value:.0f}"

# ===============================
# ITM / OTM LOGIC
# ===============================
def classify_strike(strike, option_type, future_price):

    strike = float(strike)
    future_price = float(future_price)

    if option_type == "CE":
        return "ITM" if strike < future_price else "OTM"
    elif option_type == "PE":
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
    future_match = re.search(r"FUTURE\s+PRICE:\s*([\d.]+)", text_upper)

    if not (symbol_match and lot_match):
        return None

    symbol_full = symbol_match.group(1)
    lots = int(lot_match.group(1))
    price = float(price_match.group(1)) if price_match else None
    future_price = float(future_match.group(1)) if future_match else None

    base_symbol = next((s for s in TRACK_SYMBOLS if s in symbol_full), None)
    if not base_symbol:
        return None

    # Improved strike extraction: find the digits after the Month+Year (e.g., MAR26)
    opt_match = re.search(r"(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}(\d+)(?:CE|PE)$", symbol_full)

    zone = None
    option_type = None

    if opt_match and future_price:
        strike = opt_match.group(1)
        option_type = re.search(r"(CE|PE)$", symbol_full).group(1)
        zone = classify_strike(strike, option_type, future_price)

    action_type = None

    if "WRITER" in text_upper:
        if option_type == "CE":
            action_type = "CALL_WRITER"
        elif option_type == "PE":
            action_type = "PUT_WRITER"

    elif "CALL BUY" in text_upper:
        action_type = "CALL_BUY"

    elif "PUT BUY" in text_upper:
        action_type = "PUT_BUY"

    elif "SHORT COVERING" in text_upper:
        if symbol_full.endswith("-I"): action_type = "FUTURE_SC"
        else: action_type = "CALL_SC" if option_type == "CE" else "PUT_SC"

    elif "LONG UNWINDING" in text_upper:
        if symbol_full.endswith("-I"): action_type = "FUTURE_UNW"
        else: action_type = "CALL_UNW" if option_type == "CE" else "PUT_UNW"

    elif "FUTURE BUY" in text_upper:
        action_type = "FUTURE_BUY"

    elif "FUTURE SELL" in text_upper:
        action_type = "FUTURE_SELL"

    if not action_type:
        return None

    return {
        "symbol": base_symbol,
        "lots": lots,
        "zone": zone,
        "action_type": action_type,
        "future": future_price,
        "price": price
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
# BIAS LOGIC
# ===============================
def get_bias_label(net_lots):
    if net_lots > 500: return "🔥 VERY STRONG BULLISH"
    elif net_lots > 150: return "🚀 STRONG BULLISH"
    elif net_lots > 0: return "🟢 Mild Bullish"
    elif net_lots < -500: return "🔥 VERY STRONG BEARISH"
    elif net_lots < -150: return "📉 STRONG BEARISH"
    elif net_lots < 0: return "🔴 Mild Bearish"
    else: return "⚖ Neutral"

# ===============================
# SUMMARY PROCESS (5 MIN VERSION)
# ===============================
async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer: return

    batch = list(alerts_buffer)
    alerts_buffer.clear()

    # Data structures per symbol
    opt_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    opt_turn = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    fut_data = defaultdict(lambda: defaultdict(int))
    fut_turn = defaultdict(lambda: defaultdict(float))
    last_future = {}

    for alert in batch:
        sym, act, zone, lots, price = alert["symbol"], alert["action_type"], alert["zone"], alert["lots"], alert["price"]
        lot_size = LOT_SIZES.get(sym, 1)
        if alert["future"]: last_future[sym] = alert["future"]

        if zone: # It's an Option
            opt_data[sym][act][zone] += lots
            
            # --- NEW OPTION TURNOVER LOGIC ---
            if "WRITER" in act or "_SC" in act:
                # Margin-based fixed turnover
                multiplier = 100000 if zone == "ITM" else 50000
                opt_turn[sym][act][zone] += (lots * multiplier)
            else:
                # Premium-based actual turnover
                if price: opt_turn[sym][act][zone] += (lots * price * lot_size)
        else: # It's a Future
            fut_data[sym][act] += lots
            # Fixed logic: 100,000 per lot for all Future actions
            fut_turn[sym][act] += (lots * 100000)

    message = "<pre>\n📊 5 MIN INSTITUTIONAL FLOW REPORT\n\n"

    for symbol in TRACK_SYMBOLS:
        if symbol not in opt_data and symbol not in fut_data: continue

        message += f"💎 {symbol} (FUT: {last_future.get(symbol,'N/A')})\n"
        
        # --- OPTIONS SECTION ---
        if symbol in opt_data:
            message += "--- OPTIONS FLOW ---\n"
            message += f"{'TYPE':10}{'ITM':>15}{'OTM':>15}{'TOT':>15}\n"
            message += "-" * 55 + "\n"
            
            s_bull_lots, s_bear_lots = 0, 0
            s_bull_turnover, s_bear_turnover = 0, 0
            for act in opt_data[symbol]:
                itm_l, otm_l = opt_data[symbol][act]["ITM"], opt_data[symbol][act]["OTM"]
                itm_t, otm_t = opt_turn[symbol][act]["ITM"], opt_turn[symbol][act]["OTM"]
                tot_l, tot_t = itm_l + otm_l, itm_t + otm_t
                
                if act in ["PUT_WRITER","CALL_BUY","CALL_SC","PUT_UNW"]: 
                    s_bull_lots += tot_l
                    s_bull_turnover += tot_t
                else: 
                    s_bear_lots += tot_l
                    s_bear_turnover += tot_t

                # Restore Lot(Turnover) format
                itm_str = f"{itm_l}({format_money(itm_t)})"
                otm_str = f"{otm_l}({format_money(otm_t)})"
                tot_str = f"{tot_l}({format_money(tot_t)})"
                
                message += f"{act[:10]:10}{itm_str:>15}{otm_str:>15}{tot_str:>15}\n"
            
            opt_net = s_bull_lots - s_bear_lots
            message += "-" * 55 + "\n"
            message += f"Option Bias: {get_bias_label(opt_net)}\n"
            message += f"Bullish Turn: {format_money(s_bull_turnover)}\n"
            message += f"Bearish Turn: {format_money(s_bear_turnover)}\n\n"

        # --- FUTURES SECTION ---
        if symbol in fut_data:
            message += "--- FUTURES FLOW ---\n"
            f_bull_lots, f_bear_lots = 0, 0
            f_bull_turnover, f_bear_turnover = 0, 0
            for act in fut_data[symbol]:
                lots = fut_data[symbol][act]
                turn = fut_turn[symbol][act]
                # FUTURE_BUY and FUTURE_SC are Bullish
                if act in ["FUTURE_BUY", "FUTURE_SC"]: 
                    f_bull_lots += lots
                    f_bull_turnover += turn
                # FUTURE_SELL and FUTURE_UNW are Bearish
                else: 
                    f_bear_lots += lots
                    f_bear_turnover += turn
                message += f"{act:12} : {lots} lots ({format_money(turn)})\n"
            
            fut_net = f_bull_lots - f_bear_lots
            message += f"Future Bias: {get_bias_label(fut_net)}\n"
            message += f"Bullish Turn: {format_money(f_bull_turnover)}\n"
            message += f"Bearish Turn: {format_money(f_bear_turnover)}\n"
        
        message += "=" * 50 + "\n\n"

    message += "Validity: Next 5 Minutes\n"
    message += "</pre>"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message, parse_mode="HTML")


# ===============================
# MAIN
# ===============================
def main():

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))

    if app.job_queue:
        app.job_queue.run_repeating(process_summary, interval=300, first=10)

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
