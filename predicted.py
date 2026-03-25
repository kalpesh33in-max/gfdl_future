import os
import re
import time
import logging
import asyncio
from collections import deque
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

# Load environment variables for local testing
load_dotenv()

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv('SUMMARIZER_BOT_TOKEN')
SOURCE_CHANNEL_ID = os.getenv('TARGET_CHANNEL_ID') 
DEST_CHAT_ID = os.getenv('SUMMARY_CHAT_ID')      

# Logic Settings
TIME_WINDOW_SECONDS = 300  # 5 Minutes for normal signals
BLAST_WINDOW_SECONDS = 120 # 2 Minutes for Blast signals
OI_WALL_THRESHOLD = 400000 # 400k lots is a major wall
PRICE_BUFFER = 100         # Price must be 100 pts away from Wall
MOVE_CONFIRMATION = 50     # Future must move 50 pts in 2 mins
HEAVYWEIGHTS = ['HDFCBANK', 'ICICIBANK', 'SBIN', 'AXISBANK']
BNF_SYMBOLS = ['BANKNIFTY', 'NFO:BANKNIFTY']

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Memory to hold signals and price history
signal_memory = deque()
price_history = deque() # Holds (timestamp, price)

# --- HELPER FUNCTIONS ---

def parse_message(text):
    """Parses the raw Telegram message format including Price and OI."""
    try:
        text_upper = text.upper()
        # Extract Rating
        rating_match = re.search(r'(🚀 BLAST 🚀|🌟 AWESOME|✅ VERY GOOD|⚡ GOOD)', text_upper)
        rating = rating_match.group(1) if rating_match else "NORMAL"
        
        # Extract Action
        action_match = re.search(r'🚨\s*(.*?)\s*(?:🔵|🔴|📈|📉|⤵️|⤴️|✍️|↗️|↘️)', text_upper)
        action = action_match.group(1).strip() if action_match else None
        
        # Extract Symbol
        symbol_match = re.search(r'SYMBOL:\s*([A-Z0-9:]+)', text_upper)
        symbol = symbol_match.group(1).strip() if symbol_match else None

        # Extract Price & OI
        price_match = re.search(r'FUTURE PRICE:\s*([\d.]+)', text_upper)
        price = float(price_match.group(1)) if price_match else None
        
        oi_match = re.search(r'EXISTING OI:\s*([\d,]+)', text_upper)
        oi = int(oi_match.group(1).replace(',', '')) if oi_match else 0

        if action and symbol:
            return {
                'timestamp': time.time(),
                'rating': rating,
                'action': action,
                'symbol': symbol,
                'price': price,
                'oi': oi
            }
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
    return None

def get_price_move():
    """Calculates price change in last 2 minutes."""
    current_time = time.time()
    while price_history and current_time - price_history[0][0] > 120:
        price_history.popleft()
    if len(price_history) < 2: 
        return 0
    return price_history[-1][1] - price_history[0][1]

def evaluate_sure_shot():
    """The 'Sure Shot' Logic Engine with OI & Price Filters."""
    current_time = time.time()
    
    # Clean up old signals
    while signal_memory and current_time - signal_memory[0]['timestamp'] > TIME_WINDOW_SECONDS:
        signal_memory.popleft()

    # Track structural walls and current index state
    high_oi_walls = []
    latest_bnf_price = None

    for sig in signal_memory:
        if any(b in sig['symbol'] for b in BNF_SYMBOLS):
            latest_bnf_price = sig['price']
            if sig['oi'] > OI_WALL_THRESHOLD:
                # Identify the strike from symbol (e.g., BANKNIFTY26MAR54000CE -> 54000)
                strike_match = re.search(r'(\d{5})', sig['symbol'])
                if strike_match:
                    high_oi_walls.append({
                        'strike': float(strike_match.group(1)),
                        'type': 'CE' if 'CE' in sig['symbol'] else 'PE'
                    })

    if not latest_bnf_price: 
        return None

    # Bullish (CE) Indicators
    ce_heavy_fuel = False
    ce_bnf_floor = False  
    ce_bnf_aggression = False 
    
    # Bearish (PE) Indicators
    pe_heavy_fuel = False
    pe_bnf_ceiling = False 
    pe_bnf_aggression = False 

    price_move = get_price_move()

    for sig in signal_memory:
        # Window logic: Blast signals only valid for 2 mins, Others for 5 mins
        is_blast = 'BLAST' in sig['rating']
        sig_age = current_time - sig['timestamp']
        if is_blast and sig_age > BLAST_WINDOW_SECONDS: 
            continue

        is_high = any(x in sig['rating'] for x in ['BLAST', 'AWESOME', 'VERY GOOD'])
        
        # --- BULLISH (CE) LOGIC ---
        if any(h in sig['symbol'] for h in HEAVYWEIGHTS):
            if any(act in sig['action'] for act in ['FUTURE BUY', 'SHORT COVERING']) and is_high:
                ce_heavy_fuel = True
        
        if any(b in sig['symbol'] for b in BNF_SYMBOLS):
            if 'PUT WRITER' in sig['action'] and is_high:
                ce_bnf_floor = True
            if 'CALL BUY' in sig['action'] and is_high:
                ce_bnf_aggression = True

        # --- BEARISH (PE) LOGIC ---
        if any(h in sig['symbol'] for h in HEAVYWEIGHTS):
            if any(act in sig['action'] for act in ['FUTURE SELL', 'LONG UNWINDING']) and is_high:
                pe_heavy_fuel = True
        
        if any(b in sig['symbol'] for b in BNF_SYMBOLS):
            if 'CALL WRITER' in sig['action'] and is_high:
                pe_bnf_ceiling = True
            if 'PUT BUY' in sig['action'] and is_high:
                pe_bnf_aggression = True

    # --- OI WALL FILTERS ---
    ce_blocked = False
    pe_blocked = False
    for wall in high_oi_walls:
        # Block CE if price is approaching a CE wall from below
        if wall['type'] == 'CE':
            if (wall['strike'] - latest_bnf_price) < PRICE_BUFFER and latest_bnf_price < wall['strike']:
                ce_blocked = True
        # Block PE if price is approaching a PE wall from above
        if wall['type'] == 'PE':
            if (latest_bnf_price - wall['strike']) < PRICE_BUFFER and latest_bnf_price > wall['strike']:
                pe_blocked = True

    # --- FINAL TRIGGER ---
    # Trigger CE if Fuel + Support + Aggression are present, and not hitting a CE Wall
    if ce_heavy_fuel and ce_bnf_floor and ce_bnf_aggression and not ce_blocked:
        if price_move >= MOVE_CONFIRMATION:
            return f"🟢 <b>SURE SHOT BUY: BANKNIFTY CALL (CE)</b> 🟢\nPrice: {latest_bnf_price}\nLogic: Heavyweight Blast + OI Support + Price Breakout (2m Window)"

    # Trigger PE if Fuel + Resistance + Aggression are present, and not hitting a PE Wall
    if pe_heavy_fuel and pe_bnf_ceiling and pe_bnf_aggression and not pe_blocked:
        if price_move <= -MOVE_CONFIRMATION:
            return f"🔴 <b>SURE SHOT BUY: BANKNIFTY PUT (PE)</b> 🔴\nPrice: {latest_bnf_price}\nLogic: Heavyweight Blast + OI Resistance + Price Breakdown (2m Window)"

    return None

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles incoming messages from the source channel."""
    msg = update.channel_post or update.message
    if not msg or not msg.text:
        return

    # Only process messages from the specific Source Channel
    if str(msg.chat_id) != str(SOURCE_CHANNEL_ID):
        return

    parsed = parse_message(msg.text)
    if parsed:
        logger.info(f"Signal Added: {parsed['rating']} | {parsed['action']} | {parsed['symbol']}")
        signal_memory.append(parsed)
        
        # Track BNF price history for confirmation
        if any(b in parsed['symbol'] for b in BNF_SYMBOLS) and parsed['price']:
            price_history.append((parsed['timestamp'], parsed['price']))
        
        alert_text = evaluate_sure_shot()
        if alert_text:
            await context.bot.send_message(chat_id=DEST_CHAT_ID, text=alert_text, parse_mode="HTML")
            signal_memory.clear() # Prevent double alerts
            # Note: We don't clear price_history to maintain the 2m trend

def main():
    if not BOT_TOKEN:
        logger.error("SUMMARIZER_BOT_TOKEN missing!")
        return

    logger.info("🚀 Predicted Scanner starting with Bot Token...")
    app = Application.builder().token(BOT_TOKEN).build()

    # Listener for the source channel
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.TEXT, message_handler))

    logger.info("Scanner is now active and monitoring with OI Wall & Price Filters...")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
