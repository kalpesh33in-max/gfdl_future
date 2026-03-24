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

# --- CONFIGURATION (Using your Railway Variables) ---
BOT_TOKEN = os.getenv('SUMMARIZER_BOT_TOKEN')
SOURCE_CHANNEL_ID = os.getenv('TARGET_CHANNEL_ID') # Where the bot reads raw data
DEST_CHAT_ID = os.getenv('SUMMARY_CHAT_ID')      # Where the bot sends Sure-Shot alerts

# Logic Settings
TIME_WINDOW_SECONDS = 300  # 5 Minutes
HEAVYWEIGHTS = ['HDFCBANK', 'ICICIBANK', 'SBIN', 'AXISBANK']
BNF_SYMBOLS = ['BANKNIFTY', 'NFO:BANKNIFTY']

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Memory to hold signals
signal_memory = deque()

# --- HELPER FUNCTIONS ---

def parse_message(text):
    """Parses the raw Telegram message format."""
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

        if action and symbol:
            return {
                'timestamp': time.time(),
                'rating': rating,
                'action': action,
                'symbol': symbol
            }
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
    return None

def evaluate_sure_shot():
    """The 'Sure Shot' Logic Engine."""
    current_time = time.time()
    
    # Clean up old signals (older than 5 mins)
    while signal_memory and current_time - signal_memory[0]['timestamp'] > TIME_WINDOW_SECONDS:
        signal_memory.popleft()

    # Bullish (CE) Indicators
    ce_heavy_fuel = False
    ce_bnf_floor = False  
    ce_bnf_aggression = False 
    
    # Bearish (PE) Indicators
    pe_heavy_fuel = False
    pe_bnf_ceiling = False 
    pe_bnf_aggression = False 

    for sig in signal_memory:
        # High-Signal Rating Check
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

    # --- FINAL TRIGGER ---
    if ce_heavy_fuel and ce_bnf_floor and ce_bnf_aggression:
        return "🟢 <b>SURE SHOT BUY: BANKNIFTY CALL (CE)</b> 🟢\nLogic: Heavyweight Fuel + Put Writing + Call Buying (5m Window)"

    if pe_heavy_fuel and pe_bnf_ceiling and pe_bnf_aggression:
        return "🔴 <b>SURE SHOT BUY: BANKNIFTY PUT (PE)</b> 🔴\nLogic: Heavyweight Fuel + Call Writing + Put Buying (5m Window)"

    return None

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles incoming messages from the source channel."""
    msg = update.channel_post or update.message
    if not msg or not msg.text:
        return

    # Security check: Only process messages from your specific Source Channel
    if str(msg.chat_id) != str(SOURCE_CHANNEL_ID):
        return

    parsed = parse_message(msg.text)
    if parsed:
        logger.info(f"Signal Added: {parsed['rating']} | {parsed['action']} | {parsed['symbol']}")
        signal_memory.append(parsed)
        
        alert_text = evaluate_sure_shot()
        if alert_text:
            await context.bot.send_message(chat_id=DEST_CHAT_ID, text=alert_text, parse_mode="HTML")
            signal_memory.clear() # Prevent multiple alerts for the same move

def main():
    if not BOT_TOKEN:
        logger.error("SUMMARIZER_BOT_TOKEN missing!")
        return

    logger.info("🚀 Predicted Scanner starting with Bot Token...")
    app = Application.builder().token(BOT_TOKEN).build()

    # Listener for the source channel
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.TEXT, message_handler))

    logger.info("Scanner is now active and monitoring your Admin channel...")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
