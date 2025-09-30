import os
import time
import requests
import logging

from dhanhq import DhanFeed
from dhanhq.marketfeed import NSE

# --- 1. Configuration ---
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

STOCK_ID = '1333'
STOCK_NAME = "HDFCBANK"
SEND_INTERVAL_SECONDS = 60

# --- Basic Setup ---
instruments = [(NSE, STOCK_ID)]
last_telegram_send_time = time.time()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 2. Telegram Function ---
def send_telegram_message(ltp_price):
    global last_telegram_send_time
    # ... (This function remains the same)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S IST")
    message = (
        f"🔔 *{STOCK_NAME} LTP ALERT!* 🔔\n\n"
        f"**वेळ:** {timestamp}\n"
        f"**नवीनतम LTP:** ₹ *{ltp_price:.2f}*\n\n"
        f"_हा ॲलर्ट दर {SEND_INTERVAL_SECONDS} सेकंदांनी WebSocket डेटावर आधारित आहे._"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Telegram alert sent: {STOCK_NAME} LTP @ ₹{ltp_price:.2f}")
        last_telegram_send_time = time.time()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending Telegram message: {e}")

# --- 3. WebSocket Callback Functions ---
def on_connect(instance):
    logging.info("WebSocket Connected (Old Library Version).")

def on_message(instance, message):
    try:
        # Assuming old message format
        if message.get('securityId') == STOCK_ID and message.get('lastTradedPrice'):
            ltp = message.get('lastTradedPrice')
            if ltp is not None:
                current_time = time.time()
                if current_time - last_telegram_send_time >= SEND_INTERVAL_SECONDS:
                    send_telegram_message(ltp)
    except Exception as e:
        logging.error(f"Error in on_message handler: {e}")

def on_error(instance, error):
    logging.error(f"WebSocket Error: {error}")

# --- 4. Main Function (Synchronous, for OLD library) ---
def main():
    if not all([CLIENT_ID, ACCESS_TOKEN]):
        logging.error("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN.")
        return

    logging.info(f"Starting DhanHQ WebSocket Service for {STOCK_NAME} (Old Library Mode)...")
    
    # Instantiate the class WITHOUT 'feed_type'
    feed = DhanFeed(
        client_id=CLIENT_ID,
        access_token=ACCESS_TOKEN,
        instruments=instruments
    )

    # Assign callbacks as attributes
    feed.on_connect = on_connect
    feed.on_message = on_message
    feed.on_error = on_error

    # Run the connection loop
    feed.run_forever()

# --- 5. Entry Point ---
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")
    except Exception as e:
        logging.critical(f"A critical error occurred: {e}")
