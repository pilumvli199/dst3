#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Manual WebSocket fallback for DhanHQ market feed.
- Uses dhanhq.DhanFeed to build handshake bytes (create_header/create_subscription_packet)
- Attempts to discover market_feed_wss from dhanhq.marketfeed
- Opens websocket manually and sends header+subscription bytes
- Prints received messages and sends Telegram alerts for LTP
"""

import os
import time
import logging
import traceback
import asyncio
import requests

# must be available in the Railway venv already; add to requirements if not
import websockets

import dhanhq
from dhanhq import DhanFeed
import dhanhq.marketfeed as mf_mod  # to try discover market_feed_wss, constants, etc.

# ----------------- config -----------------
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
HDFC_ID = os.environ.get("HDFC_ID", "1333")
SEND_INTERVAL_SECONDS = int(os.environ.get("SEND_INTERVAL_SECONDS", "60"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dhan-manual")

# ----------------- telegram helper -----------------
_last_sent = {}
def send_telegram(ltp):
    now = time.time()
    last = _last_sent.get(HDFC_ID, 0)
    if now - last < SEND_INTERVAL_SECONDS:
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram env missing")
        return
    text = f"*HDFC BANK LTP ALERT!* 🔔\nवेळ: {time.strftime('%H:%M:%S IST')}\n\nनवीनतम LTP: ₹ *{ltp:.2f}*"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}, timeout=8)
        if r.ok:
            _last_sent[HDFC_ID] = now
            logger.info("Telegram sent: ₹%.2f", ltp)
        else:
            logger.warning("Telegram failed: %s %s", r.status_code, r.text)
    except Exception as e:
        logger.exception("Telegram exception: %s", e)

# ----------------- helper to discover ws url -----------------
def discover_ws_url():
    # Try to get market_feed_wss variable from dhanhq.marketfeed
    if hasattr(mf_mod, "market_feed_wss"):
        candidate = getattr(mf_mod, "market_feed_wss")
        # it could be a string constant or a function; try both
        if isinstance(candidate, str):
            logger.info("Discovered market_feed_wss (string) from module.")
            return candidate
        if callable(candidate):
            try:
                # Some libs expose a function that returns url given args — try safe calls (no network)
                out = candidate(CLIENT_ID, ACCESS_TOKEN, [])
                if isinstance(out, str):
                    logger.info("Discovered market_feed_wss via callable.")
                    return out
            except Exception as e:
                logger.debug("market_feed_wss callable invocation failed: %s", e)
    # fallback: check dhanhq.marketfeed module dict for common names
    for name in ("MARKET_FEED_WSS", "MARKET_FEED_URL", "market_feed_url"):
        if hasattr(mf_mod, name):
            val = getattr(mf_mod, name)
            if isinstance(val, str):
                logger.info("Discovered ws url via attribute %s", name)
                return val
    logger.warning("Could not discover market_feed_wss URL from dhanhq.marketfeed module.")
    return None

# ----------------- main manual connection coroutine -----------------
async def manual_run():
    if not CLIENT_ID or not ACCESS_TOKEN:
        logger.critical("Missing CLIENT_ID or ACCESS_TOKEN")
        return

    # instantiate feed object (we only use its create_header/create_subscription_packet)
    try:
        feed = DhanFeed(client_id=CLIENT_ID, access_token=ACCESS_TOKEN, instruments=[(mf_mod.NSE if hasattr(mf_mod,'NSE') else 1, HDFC_ID)])
    except Exception:
        # try positional
        feed = DhanFeed(CLIENT_ID, ACCESS_TOKEN, [(mf_mod.NSE if hasattr(mf_mod,'NSE') else 1, HDFC_ID)])

    # attempt to call authorize if exists (await if coroutine)
    try:
        if hasattr(feed, "authorize") and callable(feed.authorize):
            import inspect
            if inspect.iscoroutinefunction(feed.authorize):
                logger.info("Awaiting feed.authorize() (coroutine)...")
                try:
                    # run on feed.loop if available
                    loop = getattr(feed, "loop", None)
                    if loop and isinstance(loop, asyncio.AbstractEventLoop):
                        loop.run_until_complete(feed.authorize())
                    else:
                        await feed.authorize()
                except Exception as e:
                    logger.warning("feed.authorize() raised: %s", e)
            else:
                try:
                    feed.authorize()
                except Exception as e:
                    logger.warning("feed.authorize() (sync) raised: %s", e)
    except Exception:
        logger.exception("authorize attempt failed")

    # discover ws url
    ws_url = discover_ws_url()
    if not ws_url:
        logger.error("No market_feed_wss URL found — can't continue manual mode.")
        return
    logger.info("Using WS URL: %s", ws_url)

    # prepare header bytes and subscription packet bytes using feed helpers (choose sensible code/mlen)
    # we will try code=1 and mlen=0 first (based on earlier diagnostics)
    header_bytes = None
    sub_pkt = None
    tried = []
    for code in (1,2,3,4,10):
        for mlen in (0,128,1024):
            try:
                hb = None
                try:
                    hb = feed.create_header(code, mlen, CLIENT_ID)
                except TypeError:
                    # some libs may accept int client id
                    try:
                        hb = feed.create_header(code, mlen, int(CLIENT_ID))
                    except Exception:
                        hb = None
                if hb:
                    header_bytes = hb
                    logger.info("Selected header bytes from code=%s mlen=%s (len=%d)", code, mlen, len(hb))
                    break
            except Exception as e:
                logger.debug("create_header(code=%s,mlen=%s) failed: %s", code, mlen, e)
        if header_bytes:
            break

    # subscription packet
    try:
        for code in (1,2,3,4,10):
            try:
                pkt = feed.create_subscription_packet(code)
                if pkt:
                    sub_pkt = pkt
                    logger.info("Selected subscription packet from code=%s (len=%d)", code, len(pkt) if hasattr(pkt,'__len__') else 0)
                    break
            except Exception as e:
                logger.debug("create_subscription_packet(code=%s) failed: %s", code, e)
    except Exception:
        logger.debug("subscription packet attempts failed")

    if not header_bytes:
        logger.warning("No header bytes available; library may require internal sequence. We'll still try with Authorization header.")
    # Build extra_headers for HTTP handshake (some servers expect Authorization)
    extra_headers = [("Authorization", f"Bearer {ACCESS_TOKEN}")]
    # also try custom header names
    extra_headers.append(("access-token", ACCESS_TOKEN))

    # connect and perform handshake/send header bytes
    try:
        logger.info("Connecting websocket (manual)...")
        # set ssl=None letting websockets pick default; if TLS required use ws_url as wss://...
        async with websockets.connect(ws_url, extra_headers=extra_headers, max_size=None) as ws:
            logger.info("WebSocket connected (manual). Sending header bytes if present...")
            # If header_bytes are binary bytes, send as bytes
            if header_bytes:
                try:
                    await ws.send(header_bytes)
                    logger.info("Sent header bytes (len=%d).", len(header_bytes))
                except Exception as e:
                    logger.exception("Failed to send header bytes: %s", e)
            # send subscription packet if present
            if sub_pkt:
                try:
                    await ws.send(sub_pkt)
                    logger.info("Sent subscription packet (len=%d).", len(sub_pkt))
                except Exception as e:
                    logger.exception("Failed to send subscription packet: %s", e)

            # now receive loop (prints and alerts)
            logger.info("Entering receive loop (Ctrl+C to stop).")
            try:
                while True:
                    msg = await ws.recv()
                    # msg may be bytes or str; try decode
                    data = msg
                    if isinstance(msg, (bytes, bytearray)):
                        # attempt to decode to text for simple inspection (may be binary)
                        try:
                            data = msg.decode("utf-8", errors="ignore")
                        except Exception:
                            data = repr(msg)
                    logger.debug("RECV: %s", data)
                    # simple LTP parse attempt (if text JSON)
                    try:
                        import json
                        j = None
                        if isinstance(msg, (bytes, bytearray)):
                            s = msg.decode("utf-8", errors="ignore")
                            j = json.loads(s) if s and s.strip().startswith("{") else None
                        elif isinstance(msg, str) and msg.strip().startswith("{"):
                            j = json.loads(msg)
                        if isinstance(j, dict):
                            sec = j.get("securityId") or j.get("symbol") or j.get("s")
                            ltp = j.get("lastTradedPrice") or j.get("ltp")
                            if sec and str(sec) == str(HDFC_ID) and ltp:
                                try:
                                    l = float(ltp)
                                    send_telegram(l)
                                except Exception:
                                    pass
                    except Exception:
                        pass
            except websockets.ConnectionClosed as cc:
                logger.warning("WebSocket closed: %s", cc)
    except Exception as e:
        logger.exception("Manual websocket connection failed: %s", e)

# ----------------- entry -----------------
if __name__ == "__main__":
    try:
        asyncio.run(manual_run())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception:
        logger.exception("Fatal error in manual_run")
