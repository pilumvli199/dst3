#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dhan_websocket_alert_bot.py — OLD-LIB mode with authorize-await before connect
- Calls feed.authorize() and awaits if coroutine
- Then runs diagnostics (create_header/create_subscription_packet)
- Then attempts subscribe and run_forever()
"""

import os
import time
import requests
import logging
import traceback
import inspect
import asyncio

from dhanhq import DhanFeed
from dhanhq.marketfeed import NSE

# --- 1. Configuration ---
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

STOCK_ID = '1333'
STOCK_NAME = "HDFCBANK"
SEND_INTERVAL_SECONDS = int(os.environ.get("SEND_INTERVAL_SECONDS", "60"))

# --- Basic Setup ---
instruments = [(NSE, STOCK_ID)]
last_telegram_send_time = time.time()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Telegram helper (same as before) ---
def send_telegram_message(ltp_price):
    global last_telegram_send_time
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S IST")
    message = (
        f"🔔 *{STOCK_NAME} LTP ALERT!* 🔔\n\n"
        f"**वेळ:** {timestamp}\n"
        f"**नवीनतम LTP:** ₹ *{ltp_price:.2f}*\n\n"
        f"_हा ॲलर्ट दर {SEND_INTERVAL_SECONDS} सेकंदांनी WebSocket डेटावर आधारित आहे._"
    )
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram env missing; cannot send alerts.")
        return
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
def on_connect(instance=None):
    logging.info("WebSocket Connected (Old Library Version).")

def on_message(instance, message):
    try:
        if isinstance(message, dict):
            sec = message.get('securityId') or message.get('symbol') or message.get('s')
            ltp = message.get('lastTradedPrice') or message.get('ltp')
        else:
            sec = getattr(message, 'securityId', None) or getattr(message, 'symbol', None)
            ltp = getattr(message, 'lastTradedPrice', None) or getattr(message, 'ltp', None)

        if sec and str(sec) == str(STOCK_ID) and ltp:
            try:
                ltp_val = float(ltp)
            except Exception:
                return
            current_time = time.time()
            if current_time - last_telegram_send_time >= SEND_INTERVAL_SECONDS:
                send_telegram_message(ltp_val)
    except Exception as e:
        logging.error(f"Error in on_message handler: {e}\n{traceback.format_exc()}")

def on_error(instance, error):
    logging.error(f"WebSocket Error: {error}")

# --- Diagnostic helper ---
def log_feed_diagnostics(feed, instruments):
    try:
        if hasattr(feed, "ws"):
            try:
                logging.info("DIAG: feed.ws -> %s", getattr(feed, "ws"))
            except Exception:
                logging.debug("Cannot read feed.ws")
        if hasattr(feed, "create_header") and callable(getattr(feed, "create_header")):
            logging.info("DIAG: feed.create_header() is callable (requires args).")
        if hasattr(feed, "create_subscription_packet") and callable(getattr(feed, "create_subscription_packet")):
            logging.info("DIAG: feed.create_subscription_packet() is callable (requires args).")
    except Exception:
        logging.exception("DIAG: diagnostics failed")

# --- Helper to run coroutine with feed.loop if available, otherwise asyncio.run ---
def run_coro_on_feed(feed, coro):
    loop = getattr(feed, "loop", None)
    if loop and isinstance(loop, asyncio.AbstractEventLoop):
        return loop.run_until_complete(coro)
    else:
        return asyncio.run(coro)

# --- Main function with explicit authorize-await before connect ---
def main():
    if not all([CLIENT_ID, ACCESS_TOKEN]):
        logging.error("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN.")
        return

    logging.info(f"Starting DhanHQ WebSocket Service for {STOCK_NAME} (Old Library Mode) with authorize step...")

    # instantiate
    try:
        feed = DhanFeed(client_id=CLIENT_ID, access_token=ACCESS_TOKEN, instruments=instruments)
    except Exception as e:
        logging.warning("Constructor kwargs failed or raised exception (%s); trying positional constructor.", e)
        try:
            feed = DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments)
        except Exception as ex:
            logging.critical("Failed to instantiate DhanFeed: %s", ex)
            logging.critical("Traceback:\n%s", traceback.format_exc())
            return

    # set callbacks
    try:
        feed.on_connect = on_connect
    except Exception:
        logging.debug("Could not set feed.on_connect attribute directly.")
    try:
        feed.on_message = on_message
    except Exception:
        logging.debug("Could not set feed.on_message attribute directly.")
    try:
        feed.on_error = on_error
    except Exception:
        logging.debug("Could not set feed.on_error attribute directly.")

    # --- IMPORTANT: AUTHORIZE (await if coroutine) ---
    try:
        if hasattr(feed, "authorize") and callable(getattr(feed, "authorize")):
            auth_fn = getattr(feed, "authorize")
            if inspect.iscoroutinefunction(auth_fn):
                logging.info("authorize() is coroutine — awaiting it now...")
                try:
                    run_coro_on_feed(feed, auth_fn())
                    logging.info("feed.authorize() awaited successfully.")
                except Exception as e:
                    logging.exception("feed.authorize() coroutine raised: %s", e)
            else:
                try:
                    auth_fn()
                    logging.info("feed.authorize() called (sync).")
                except TypeError:
                    try:
                        auth_fn(ACCESS_TOKEN)
                        logging.info("feed.authorize(ACCESS_TOKEN) called (sync).")
                    except Exception as e:
                        logging.debug("feed.authorize sync variants failed: %s", e)
    except Exception:
        logging.exception("Authorize attempt raised exception")

    # diagnostics AFTER authorize
    log_feed_diagnostics(feed, instruments)

    # --- DIAGNOSTIC: try create_header / create_subscription_packet with candidate feed_request_codes ---
    try:
        candidate_codes = [1, 2, 3, 4, 10]
        for code in candidate_codes:
            try:
                for mlen in (0, 128, 1024):
                    try:
                        hdr = None
                        try:
                            hdr = feed.create_header(code, mlen, CLIENT_ID)
                        except TypeError as te:
                            # some libs accept client_id as int or str; try both forms
                            try:
                                hdr = feed.create_header(code, mlen, str(CLIENT_ID))
                            except Exception:
                                hdr = None
                        if hdr is not None:
                            logging.info("DIAG: create_header(code=%s, mlen=%s, client_id=%s) -> %s", code, mlen, CLIENT_ID, hdr)
                    except Exception as e:
                        logging.debug("DIAG: create_header(code=%s, mlen=%s) failed: %s", code, mlen, str(e))
                if hasattr(feed, "create_subscription_packet") and callable(getattr(feed, "create_subscription_packet")):
                    try:
                        pkt = feed.create_subscription_packet(code)
                        logging.info("DIAG: create_subscription_packet(code=%s) -> %s", code, pkt)
                    except Exception as e:
                        logging.debug("DIAG: create_subscription_packet(code=%s) failed: %s", code, str(e))
            except Exception:
                logging.exception("DIAG: inner loop failure for code %s", code)
    except Exception:
        logging.exception("DIAG: create_header/create_subscription_packet diagnostic block failed")

    # If create_header produced usable bytes for any code, we CAN set feed.header or override create_header.
    # (But often these bytes are low-level handshake packets expected to be sent AFTER the websocket is opened,
    #  not as HTTP headers. The library handles that—so forcing HTTP Authorization header is often not sufficient.)
    # Still try a conservative override for HTTP header if server expects it:
    try:
        forced = {"Authorization": f"Bearer {ACCESS_TOKEN}", "access-token": ACCESS_TOKEN}
        try:
            setattr(feed, "header", forced)
            logging.info("Set feed.header to forced Authorization header.")
        except Exception:
            try:
                def _forced_create_header():
                    return forced
                setattr(feed, "create_header", _forced_create_header)
                logging.info("Overrode feed.create_header to return forced Authorization header.")
            except Exception as e:
                logging.debug("Could not override/create header: %s", e)
    except Exception:
        logging.exception("Failed setting forced header")

    # Try subscribing (numeric-ids fallback)
    try:
        try:
            ids = [int(t[1]) for t in instruments]
        except Exception:
            ids = [t[1] for t in instruments]
        if hasattr(feed, "subscribe_instruments") and callable(getattr(feed, "subscribe_instruments")):
            try:
                feed.subscribe_instruments(instruments)
                logging.info("Called feed.subscribe_instruments(instruments)")
            except Exception:
                try:
                    feed.subscribe_instruments(ids)
                    logging.info("Called feed.subscribe_instruments(ids)")
                except Exception as e:
                    logging.debug("subscribe_instruments attempts failed: %s", e)
        elif hasattr(feed, "subscribe_symbols") and callable(getattr(feed, "subscribe_symbols")):
            try:
                feed.subscribe_symbols(instruments)
                logging.info("Called feed.subscribe_symbols(instruments)")
            except Exception:
                try:
                    feed.subscribe_symbols(ids)
                    logging.info("Called feed.subscribe_symbols(ids)")
                except Exception as e:
                    logging.debug("subscribe_symbols attempts failed: %s", e)
        else:
            logging.info("No subscribe_instruments/subscribe_symbols available on feed instance.")
    except Exception:
        logging.exception("Subscription attempt failed")

    # Finally start the feed and capture handshake errors
    try:
        logging.info("Invoking feed.run_forever() ...")
        feed.run_forever()
    except Exception as e:
        logging.critical("Critical failure when starting feed: %s", e)
        logging.critical("Full traceback:\n%s", traceback.format_exc())

# Entry point
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")
    except Exception as e:
        logging.critical(f"A critical error occurred: {e}\n{traceback.format_exc()}")
