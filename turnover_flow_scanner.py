# ===============================
# SUMMARY PROCESS (Updated Logic)
# ===============================
async def process_summary(context: ContextTypes.DEFAULT_TYPE):
    global alerts_buffer
    if not alerts_buffer: return

    batch = list(alerts_buffer)
    alerts_buffer.clear()

    opt_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    opt_turn = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    fut_data = defaultdict(lambda: defaultdict(int))
    fut_turn = defaultdict(lambda: defaultdict(float))
    last_future = {}

    for alert in batch:
        sym, act, zone, lots, price = alert["symbol"], alert["action_type"], alert["zone"], alert["lots"], alert["price"]
        lot_size = LOT_SIZES.get(sym, 1)
        if alert["future"]: last_future[sym] = alert["future"]

        if zone: # Option Flow
            opt_data[sym][act][zone] += lots
            # Updated: Strictly Premium Turnover for all Option types
            if price: 
                opt_turn[sym][act][zone] += (lots * price * lot_size)
        else: # Future Flow
            fut_data[sym][act] += lots
            fut_turn[sym][act] += (lots * 175000) # Remaining same as per instructions

    message = "<pre>\n📊 2 MIN INSTITUTIONAL FLOW REPORT\n\n"

    for symbol in TRACK_SYMBOLS:
        if symbol not in opt_data and symbol not in fut_data: continue

        message += f"💎 {symbol} (FUT: {last_future.get(symbol,'N/A')})\n"
        
        if symbol in opt_data:
            message += "--- OPTIONS FLOW ---\n"
            # Preserved specific spacing requested
            message += f"{'TYPE':10}{'ITM':>13}{'OTM':>13}{'TOT':>13}\n"
            message += "-" * 49 + "\n"
            
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
                
                itm_str = f"{itm_l}({format_money(itm_t)})"
                otm_str = f"{otm_l}({format_money(otm_t)})"
                tot_str = f"{tot_l}({format_money(tot_t)})"
                
                display_act = act.replace("CALL_WRITER","CALL_WR").replace("PUT_WRITER","PUT_WR").replace("SHORT_COVERING","SC").replace("LONG_UNWINDING","UNW")
                message += f"{display_act[:10]:10}{itm_str:>13}{otm_str:>13}{tot_str:>13}\n"
            
            message += "-" * 49 + "\n"
            opt_net = s_bull_lots - s_bear_lots
            message += f"Option Bias: {get_bias_label(opt_net)}\n"
            message += f"Bullish Turn: {format_money(s_bull_turnover)}\n"
            message += f"Bearish Turn: {format_money(s_bear_turnover)}\n\n"

        if symbol in fut_data:
            message += "---- FUTURES FLOW ----\n"
            f_bull_lots, f_bear_lots = 0, 0
            f_bull_turnover, f_bear_turnover = 0, 0
            for act in fut_data[symbol]:
                lots, turn = fut_data[symbol][act], fut_turn[symbol][act]
                if act in ["FUTURE_BUY", "FUTURE_SC"]: 
                    f_bull_lots += lots
                    f_bull_turnover += turn
                else: 
                    f_bear_lots += lots
                    f_bear_turnover += turn
                message += f"{act:12} : {lots} lots ({format_money(turn)})\n"
            
            message += f"Future Bias: {get_bias_label(f_bull_lots - f_bear_lots)}\n"
            message += f"Bullish Turn: {format_money(f_bull_turnover)}\n"
            message += f"Bearish Turn: {format_money(f_bear_turnover)}\n"
        
        message += "========================================\n\n"

    message += "Validity: Next 2 Minutes\n"
    message += "</pre>"

    await context.bot.send_message(chat_id=SUMMARY_CHAT_ID, text=message, parse_mode="HTML")
