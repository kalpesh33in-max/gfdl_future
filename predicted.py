import os
import re
import time
import logging
import asyncio
from collections import deque
from telethon import TelegramClient, events
import requests
from dotenv import load_dotenv

# Load environment variables for local testing
load_dotenv()

from telethon.sessions import StringSession

# --- CONFIGURATION ---
API_ID = int(os.getenv('API_ID', 0))
API_HASH = os.getenv('API_HASH', '')
STRING_SESSION = os.getenv('STRING_SESSION', '') # New: Using StringSession for cloud
SOURCE_CHANNEL_ID = int(os.getenv('SOURCE_CHANNEL_ID', 0)) 
TARGET_BOT_TOKEN = os.getenv('TARGET_BOT_TOKEN', '')
TARGET_CHAT_ID = os.getenv('TARGET_CHAT_ID', '')

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

def send_telegram_alert(message):
    """Sends the final Sure Shot alert to your personal Telegram Bot."""
    if not TARGET_BOT_TOKEN or not TARGET_CHAT_ID:
        logger.warning("Target Bot Token or Chat ID not configured.")
        return
    
    url = f"https://api.telegram.org/bot{TARGET_BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, json={"chat_id": TARGET_CHAT_ID, "text": message, "parse_mode": "HTML"})
        if response.status_code == 200:
            logger.info("✅ Sure Shot alert sent successfully.")
        else:
            logger.error(f"❌ Failed to send alert: {response.text}")
    except Exception as e:
        logger.error(f"❌ Error sending telegram alert: {e}")

def parse_message(text):
    """
    Parses the raw Telegram message format.
    Handles ratings, actions, symbols, and lots.
    """
    try:
        # Extract Rating
        rating_match = re.search(r'(🚀 BLAST 🚀|🌟 AWESOME|✅ VERY GOOD|⚡ GOOD)', text)
        rating = rating_match.group(1) if rating_match else "NORMAL"
        
        # Extract Action (handles various emojis)
        action_match = re.search(r'🚨\s*(.*?)\s*(?:🔵|🔴|📈|📉|⤵️|⤴️|✍️|↗️|↘️)', text)
        action = action_match.group(1).strip() if action_match else None
        
        # Extract Symbol
        symbol_match = re.search(r'Symbol:\s*([A-Z0-9:]+)', text)
        symbol = symbol_match.group(1).strip() if symbol_match else None
        
        # Extract Lots
        lots_match = re.search(r'LOTS:\s*(\d+)', text)
        lots = int(lots_match.group(1)) if lots_match else 0

        if action and symbol:
            return {
                'timestamp': time.time(),
                'rating': rating,
                'action': action,
                'symbol': symbol,
                'lots': lots
            }
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
    return None

def evaluate_sure_shot():
    """
    The 'Sure Shot' Logic Engine.
    Analyzes the last 5 minutes of data for synchronized signals.
    """
    current_time = time.time()
    
    # Clean up old signals
    while signal_memory and current_time - signal_memory[0]['timestamp'] > TIME_WINDOW_SECONDS:
        signal_memory.popleft()

    # CE (Bullish) Indicators
    ce_heavy_fuel = False
    ce_bnf_floor = False  # Put Writing
    ce_bnf_aggression = False # Call Buying
    
    # PE (Bearish) Indicators
    pe_heavy_fuel = False
    pe_bnf_ceiling = False # Call Writing
    pe_bnf_aggression = False # Put Buying

    for sig in signal_memory:
        # We only trigger Sure Shot on HIGH-SIGNAL ratings
        is_high_signal = any(x in sig['rating'] for x in ['BLAST', 'AWESOME', 'VERY GOOD'])
        
        # --- BULLISH (CE) LOGIC ---
        # 1. Component Fuel
        if any(h in sig['symbol'] for h in HEAVYWEIGHTS):
            if any(act in sig['action'] for act in ['FUTURE BUY', 'SHORT COVERING']) and is_high_signal:
                ce_heavy_fuel = True
        
        # 2. BNF Floor (Put Writing)
        if any(b in sig['symbol'] for b in BNF_SYMBOLS):
            if 'PUT WRITER' in sig['action'] and is_high_signal:
                ce_bnf_floor = True
            if 'CALL BUY' in sig['action'] and is_high_signal:
                ce_bnf_aggression = True

        # --- BEARISH (PE) LOGIC ---
        # 1. Component Fuel
        if any(h in sig['symbol'] for h in HEAVYWEIGHTS):
            if any(act in sig['action'] for act in ['FUTURE SELL', 'LONG UNWINDING']) and is_high_signal:
                pe_heavy_fuel = True
        
        # 2. BNF Ceiling (Call Writing)
        if any(b in sig['symbol'] for b in BNF_SYMBOLS):
            if 'CALL WRITER' in sig['action'] and is_high_signal:
                pe_bnf_ceiling = True
            if 'PUT BUY' in sig['action'] and is_high_signal:
                pe_bnf_aggression = True

    # --- FINAL TRIGGER ---
    if ce_heavy_fuel and ce_bnf_floor and ce_bnf_aggression:
        alert_msg = "<b>🚀 SURE SHOT BUY: BANKNIFTY CALL (CE) 🚀</b>\n\n"
        alert_msg += "✅ Logic Confirmed:\n"
        alert_msg += "• Heavyweight Accumulation Detected\n"
        alert_msg += "• Massive Put Writing (Floor)\n"
        alert_msg += "• Aggressive Call Buying (Momentum)"
        send_telegram_alert(alert_msg)
        signal_memory.clear() # Avoid spamming multiple alerts for same move

    elif pe_heavy_fuel and pe_bnf_ceiling and pe_bnf_aggression:
        alert_msg = "<b>🩸 SURE SHOT BUY: BANKNIFTY PUT (PE) 🩸</b>\n\n"
        alert_msg += "✅ Logic Confirmed:\n"
        alert_msg += "• Heavyweight Distribution Detected\n"
        alert_msg += "• Massive Call Writing (Ceiling)\n"
        alert_msg += "• Aggressive Put Buying (Momentum)"
        send_telegram_alert(alert_msg)
        signal_memory.clear()

# --- MAIN RUNNER ---

async def main():
    if not API_ID or not API_HASH:
        logger.error("API_ID or API_HASH missing in environment variables.")
        return

    logger.info("Starting BankNifty Sure-Shot Scanner with StringSession...")
    
    # Initialize Telethon Client using StringSession
    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

    @client.on(events.NewMessage(chats=SOURCE_CHANNEL_ID))
    async def handler(event):
        msg_text = event.message.message
        parsed = parse_message(msg_text)
        
        if parsed:
            logger.info(f"Parsed Signal: {parsed['rating']} | {parsed['action']} | {parsed['symbol']}")
            signal_memory.append(parsed)
            evaluate_sure_shot()

    async with client:
        logger.info("Scanner is now active and monitoring Telegram...")
        await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
