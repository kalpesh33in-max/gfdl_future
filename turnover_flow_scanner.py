import os
import re
import logging
import sys
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# --- CONFIGURATION ---
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout
)

BOT_TOKEN = os.getenv("SUMMARIZER_BOT_TOKEN")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")

# Buffer stores parsed alerts for the 5-minute window
alerts_buffer = []

TRACK_SYMBOLS = ["BANKNIFTY", "HDFCBANK", "ICICIBANK", "AXISBANK", "SBIN"]

# Matches the specific display order of the Summary Bot
OPTION_DISPLAY_ORDER = [
    "CALL_WRITER",
    "CALL_SC",
    "CALL_BUY",
    "CALL_UNW",
    "PUT_BUY",
    "PUT_UNW",
    "PUT_WRITER",
    "PUT_SC",
]

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
    if net_lots > 0: return "🔥BULLISH 🚀"
    elif net_lots < 0: return "📉BEARISH📉"
    else: return "⚖ Neutral"

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
    elif "FUTURE BUY" in text_upper or "BUY (LONG)" in text_upper or (is_future and "BUY" in text_upper):
        action_type = "FUTURE_BUY"
    elif "FUTURE SELL" in text_upper or "SELL (SHORT)" in text_upper or (is_future and "SELL" in text_upper):
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
            alerts_buffer.append(parsed)

async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer:
        return

    # Clear buffer for the next 5-minute window
    batch, alerts_buffer = alerts_buffer, []
    
    opt_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    opt_turn = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    fut_data = defaultdict(lambda: defaultdict(int))
    fut_turn = defaultdict(lambda: defaultdict(float))
    last_future = {}

    for alert in batch:
        sym, act, zone, lots, price = alert["symbol"], alert["action_type"], alert["zone"], alert["lots"], alert["price"]
        lot_size = LOT_SIZES.get(sym, 1)
        if alert["future"]: last_future[sym] = alert["future"]

        if "FUTURE" not in act:
            z = zone if zone else "OTM"
            opt_data[sym][act][z] += lots
            # Match Summary Bot turnover logic
            if "WRITER" in act or "_SC" in act:
                opt_turn[sym][act][z] += (lots * 125000)
            else:
                if price: opt_turn[sym][act][z] += (lots * price * lot_size)
        else:
            fut_data[sym][act] += lots
            fut_turn[sym][act] += (lots * 175000)

    message = "<pre>\n📊 5 MIN INSTITUTIONAL FLOW REPORT\n\n"

    for symbol in TRACK_SYMBOLS:
        if symbol not in opt_data and symbol not in fut_data: continue
        
        message += f"{symbol} ({last_future.get(symbol,'N/A')}) OPTIONS FLOW\n"
        
        if symbol in opt_data:
            message += f"{'TYPE':8}{'ITM':>14}{'OTM':>14}{'TOT':>14}\n"
            message += "-" * 50 + "\n"
            
            s_bull_lots, s_bear_lots = 0, 0
            s_bull_turnover, s_bear_turnover = 0, 0
            
            # Use Fixed Order from Summary Bot
            for act in OPTION_DISPLAY_ORDER:
                if act not in opt_data[symbol]: continue
                
                itm_l, otm_l = opt_data[symbol][act]["ITM"], opt_data[symbol][act]["OTM"]
                itm_t, otm_t = opt_turn[symbol][act]["ITM"], opt_turn[symbol][act]["OTM"]
                tot_l, tot_t = itm_l + otm_l, itm_t + otm_t
                
                if act in ["PUT_WRITER", "CALL_BUY", "CALL_SC", "PUT_UNW"]: 
                    s_bull_lots += tot_l
                    s_bull_turnover += tot_t
                else: 
                    s_bear_lots += tot_l
                    s_bear_turnover += tot_t
                
                itm_s = f"{itm_l}({format_money(itm_t)})"
                otm_s = f"{otm_l}({format_money(otm_t)})"
                tot_s = f"{tot_l}({format_money(tot_t)})"
                display_act = act.replace("CALL_WRITER","CALL_WR").replace("PUT_WRITER","PUT_WR")
                message += f"{display_act:10}{itm_s:>14}{otm_s:>14}{tot_s:>14}\n"
            
            message += f"Option Bias: {get_bias_label(s_bull_lots - s_bear_lots)} {format_money(abs(s_bull_turnover - s_bear_turnover))}\n"
            message += f"Bull: {format_money(s_bull_turnover)} | Bear: {format_money(s_bear_turnover)}\n"

        if symbol in fut_data:
            message += "---- FUTURES FLOW ----\n"
            f_bull_lots, f_bear_lots = 0, 0
            
            # Comparison display style
            f_buy = f"{fut_data[symbol].get('FUTURE_BUY', 0)} ({format_money(fut_turn[symbol].get('FUTURE_BUY', 0))})"
            f_sel = f"{fut_data[symbol].get('FUTURE_SELL', 0)} ({format_money(fut_turn[symbol].get('FUTURE_SELL', 0))})"
            f_unw = f"{fut_data[symbol].get('FUTURE_UNW', 0)} ({format_money(fut_turn[symbol].get('FUTURE_UNW', 0))})"
            f_sc  = f"{fut_data[symbol].get('FUTURE_SC', 0)} ({format_money(fut_turn[symbol].get('FUTURE_SC', 0))})"
            
            message += f"F_BUY : {f_buy} == F_SEL : {f_sel}\n"
            message += f"F_UNW : {f_unw} == F_SC  : {f_sc}\n"

            for act in fut_data[symbol]:
                if act in ["FUTURE_BUY", "FUTURE_SC"]: f_bull_lots += fut_data[symbol][act]
                else: f_bear_lots += fut_data[symbol][act]
            
            message += f"Future Bias: {get_bias_label(f_bull_lots - f_bear_lots)}\n"
        
        message += "\n"

    message = message.rstrip() + "\n\nValidity: Next 5 Minutes\n</pre>"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message, parse_mode="HTML")

def main():
    if not BOT_TOKEN:
        print("Error: SUMMARIZER_BOT_TOKEN not set.")
        return
        
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    
    if app.job_queue:
        # 5-minute snapshot report
        app.job_queue.run_repeating(process_summary, interval=300, first=300)
        
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
