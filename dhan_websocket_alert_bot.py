#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dhan_websocket_alert_bot.py — updated robust version
- coroutine-aware authorize & callback handling
- forced Authorization header fallback
- numeric-id subscription fallback
- diagnostic logs for create_header / create_subscription_packet / ws
- throttled Telegram alerts for LTP
"""

import os
import time
import logging
import traceback
import inspect
from typing import Any, Optional
import asyncio
import signal

import requests

# -----------------
# Config / Env
# -----------------
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
HDFC_ID = os.environ.get("HDFC_ID", "1333")
SEND_INTERVAL_SECONDS = int(os.environ.get("SEND_INTERVAL_SECONDS", "60"))
INITIAL_BACKOFF = 1
MAX_BACKOFF = 60

# -----------------
# Logging
# -----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dhan-autodetect")

# -----------------
# Telegram helpers
# -----------------
def esc_md(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    for ch in r"_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, "\\" + ch)
    return text

_last_sent = {}
def send_telegram_message(security_id: str, ltp_price: float, friendly_name: Optional[str]=None):
    now = time.time()
    last = _last_sent.get(security_id, 0)
    if now - last < SEND_INTERVAL_SECONDS:
        logger.debug("Throttle skip %s", security_id)
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Missing telegram envs.")
        return
    timestamp = time.strftime("%H:%M:%S IST")
    name = friendly_name or f"Security {security_id}"
    message = (
        f"*{esc_md('HDFC BANK LTP ALERT!')}* 🔔\n"
        f"वेळ: {esc_md(timestamp)}\n\n"
        f"*{esc_md(name)}*\n"
        f"नवीनतम LTP: ₹ *{esc_md(f'{ltp_price:.2f}')}*\n\n"
        f"_हा ॲलर्ट दर {SEND_INTERVAL_SECONDS} सेकंदाने WebSocket Data वर आधारित आहे._"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=8)
        if resp.ok:
            logger.info("Telegram sent for %s: ₹%.2f", security_id, ltp_price)
            _last_sent[security_id] = time.time()
        else:
            logger.warning("Telegram API error %s: %s", resp.status_code, resp.text)
    except Exception as e:
        logger.exception("Telegram send failed: %s", e)

# -----------------
# Generic message handler
# -----------------
latest_ltp = {}
def market_feed_handler(message: Any):
    try:
        if isinstance(message, (bytes, str)):
            import json
            try:
                message = json.loads(message)
            except Exception:
                pass

        security_id = None
        ltp = None
        if isinstance(message, dict):
            security_id = message.get("securityId") or message.get("symbol") or message.get("security_id") or message.get("s")
            ltp = message.get("lastTradedPrice") or message.get("ltp") or message.get("last_price") or message.get("last")
            if not security_id:
                for k in ("data", "payload", "tick", "update"):
                    nested = message.get(k)
                    if isinstance(nested, dict):
                        security_id = security_id or nested.get("securityId") or nested.get("symbol")
                        ltp = ltp or nested.get("lastTradedPrice") or nested.get("ltp")
                        if security_id:
                            break
        else:
            for attr in ("securityId", "symbol", "security_id"):
                if hasattr(message, attr):
                    security_id = getattr(message, attr)
            for attr in ("lastTradedPrice", "ltp", "last_price", "last"):
                if hasattr(message, attr):
                    ltp = getattr(message, attr)

        if security_id:
            security_id = str(security_id)
        if ltp is not None:
            try:
                ltp = float(ltp)
            except Exception:
                ltp = None

        if security_id and ltp is not None:
            latest_ltp[security_id] = ltp
            logger.info("Real-time: %s LTP %.2f", security_id, ltp)
            send_telegram_message(security_id, ltp, friendly_name="HDFC BANK" if security_id == HDFC_ID else None)
        else:
            logger.debug("Ignored msg (no sec/ltp): %s", message)
    except Exception as e:
        logger.exception("Handler error: %s\n%s", e, traceback.format_exc())

# -----------------
# Constructor helper
# -----------------
def instantiate_feed_simple(feed_class, client, token, instruments, version_candidates):
    param_names = []
    try:
        sig = inspect.signature(feed_class)
        param_names = list(sig.parameters.keys())
        logger.debug("Constructor params: %s", param_names)
    except Exception:
        param_names = []

    mappings = [
        {"client_id": client, "access_token": token, "instruments": instruments},
        {"clientId": client, "access_token": token, "instruments": instruments},
        {"client": client, "token": token, "instruments": instruments},
        {"client_id": client, "token": token, "instruments": instruments},
        {"client": client, "access_token": token, "instruments": instruments},
    ]
    version_keys = ["version", "v", "feed_type", "feedType"]

    for ver in version_candidates:
        for kw in mappings:
            kwargs = {}
            for k, v in kw.items():
                if not param_names or k in param_names:
                    kwargs[k] = v
            if ver is not None:
                for vk in version_keys:
                    if not param_names or vk in param_names:
                        kwargs[vk] = ver
                        break
            if not kwargs:
                continue
            try:
                logger.info("Trying constructor kwargs: %s", list(kwargs.keys()))
                inst = feed_class(**kwargs)
                logger.info("Instantiated feed via kwargs (version=%s)", ver)
                return inst, ver
            except TypeError:
                logger.debug("Constructor kwargs TypeError; will try other forms.")
            except ValueError as ve:
                logger.warning("Constructor ValueError: %s", ve)
                if "Unsupported version" in str(ve):
                    break
            except Exception as e:
                logger.exception("Constructor raised exception: %s", e)
        try:
            if ver is None:
                logger.info("Trying constructor positional (client, token, instruments)")
                inst = feed_class(client, token, instruments)
                logger.info("Instantiated feed via positional (no version).")
                return inst, None
            else:
                logger.info("Trying constructor positional with version: %s", ver)
                inst = feed_class(client, token, instruments, ver)
                logger.info("Instantiated feed via positional with version=%s", ver)
                return inst, ver
        except TypeError:
            logger.debug("Positional instantiation TypeError for version %s", ver)
        except ValueError as ve:
            logger.warning("Constructor ValueError positional: %s", ve)
            if "Unsupported version" in str(ve):
                continue
        except Exception as e:
            logger.exception("Positional constructor exception: %s", e)
    try:
        inst = feed_class(client, token)
        logger.info("Instantiated feed via fallback (client, token).")
        return inst, None
    except Exception as e:
        logger.exception("Fallback instantiation failed: %s", e)
    return None, None

# -----------------
# Improved try_start_feed_instance handling async coroutines
# -----------------
def try_start_feed_instance(feed, instruments):
    """
    Authorize (await if coroutine), register callbacks (assign or await if needed),
    subscribe, debug headers/packets, then call run_forever() (no args).
    """
    try:
        feed_dir = dir(feed)
        logger.info("feed dir: %s", ", ".join(feed_dir))
    except Exception:
        logger.exception("Unable to list feed dir")
        feed_dir = []

    # Helper to run coroutine using feed.loop if present, otherwise asyncio.run()
    def run_coro(coro):
        try:
            loop = getattr(feed, "loop", None)
            if loop and isinstance(loop, asyncio.AbstractEventLoop):
                # Use feed.loop if present in library
                return loop.run_until_complete(coro)
            else:
                return asyncio.run(coro)
        except Exception as e:
            logger.exception("Error running coroutine: %s", e)
            raise

    # 0) Ensure access token attribute if present
    try:
        if hasattr(feed, "access_token"):
            try:
                cur = getattr(feed, "access_token", None)
                if not cur:
                    setattr(feed, "access_token", ACCESS_TOKEN)
                    logger.info("Set feed.access_token = ACCESS_TOKEN")
            except Exception:
                logger.debug("Could not set feed.access_token")
    except Exception:
        pass

    # 1) Authorize: if coroutine function, await it; else call sync
    try:
        if hasattr(feed, "authorize") and callable(getattr(feed, "authorize")):
            auth_fn = getattr(feed, "authorize")
            if inspect.iscoroutinefunction(auth_fn):
                try:
                    run_coro(auth_fn())
                    logger.info("Awaited feed.authorize() coroutine")
                except Exception as e:
                    logger.debug("Awaiting authorize failed: %s", e)
            else:
                try:
                    auth_fn()
                    logger.info("Called feed.authorize() (sync)")
                except TypeError:
                    try:
                        auth_fn(ACCESS_TOKEN)
                        logger.info("Called feed.authorize(ACCESS_TOKEN) (sync)")
                    except Exception as e:
                        logger.debug("feed.authorize sync variations failed: %s", e)
    except Exception:
        logger.exception("Authorize attempt raised exception")

    # 2) Register callbacks: detect coroutine attributes and assign handler rather than calling
    callback_names = ["on_ticks", "on_tick", "on_message", "on_data", "on_update", "on_connection_opened", "on_open", "on_connect"]
    registered = False
    for name in callback_names:
        try:
            if hasattr(feed, name):
                attr = getattr(feed, name)
                if inspect.iscoroutinefunction(attr):
                    # attribute is coroutine function - assign handler instead of calling
                    try:
                        setattr(feed, name, market_feed_handler)
                        logger.info("Assigned handler to coroutine attribute feed.%s (did not call)", name)
                        registered = True
                    except Exception as e:
                        logger.debug("Failed assigning handler to coroutine attr %s: %s", name, e)
                elif callable(attr):
                    try:
                        attr(market_feed_handler)
                        logger.info("Registered handler by calling feed.%s(handler)", name)
                        registered = True
                    except TypeError:
                        try:
                            setattr(feed, name, market_feed_handler)
                            logger.info("Registered handler by setting feed.%s = handler (fallback)", name)
                            registered = True
                        except Exception as e:
                            logger.debug("Could not set attribute %s: %s", name, e)
                    except Exception as e:
                        logger.debug("Calling feed.%s raised: %s", name, e)
                else:
                    try:
                        setattr(feed, name, market_feed_handler)
                        logger.info("Registered handler by setting feed.%s = handler (non-callable)", name)
                        registered = True
                    except Exception as e:
                        logger.debug("Could not set non-callable attr %s: %s", name, e)
        except Exception:
            logger.exception("Error registering callback on %s", name)
    if not registered:
        logger.warning("No common callback hook found (on_ticks/on_message). feed may still push via internal handlers.")

    # 3) Subscribe instruments: try tuple-list, else numeric ids
    try:
        ids = None
        try:
            ids = [int(t[1]) for t in instruments]
        except Exception:
            ids = [t[1] for t in instruments]
        if hasattr(feed, "subscribe_instruments") and callable(getattr(feed, "subscribe_instruments")):
            try:
                # try the original instruments (tuples) first
                feed.subscribe_instruments(instruments)
                logger.info("Called feed.subscribe_instruments(instruments)")
            except TypeError:
                try:
                    feed.subscribe_instruments(ids)
                    logger.info("Called feed.subscribe_instruments(ids)")
                except Exception as e:
                    logger.debug("subscribe_instruments variations failed: %s", e)
        elif hasattr(feed, "subscribe_symbols") and callable(getattr(feed, "subscribe_symbols")):
            try:
                feed.subscribe_symbols(instruments)
                logger.info("Called feed.subscribe_symbols(instruments)")
            except TypeError:
                try:
                    feed.subscribe_symbols(ids)
                    logger.info("Called feed.subscribe_symbols(ids)")
                except Exception as e:
                    logger.debug("subscribe_symbols variations failed: %s", e)
        else:
            logger.info("No subscribe_instruments/subscribe_symbols method found.")
    except Exception:
        logger.exception("Subscription attempt raised exception")

    # 4) Debug create_header, subscription packet and ws url
    try:
        if hasattr(feed, "ws"):
            try:
                logger.info("feed.ws -> %s", getattr(feed, "ws"))
            except Exception:
                pass
        if hasattr(feed, "create_header") and callable(getattr(feed, "create_header")):
            try:
                hdr = feed.create_header()
                logger.info("feed.create_header() -> %s", hdr)
            except Exception as e:
                logger.debug("feed.create_header() raised: %s", e)
        if hasattr(feed, "create_subscription_packet") and callable(getattr(feed, "create_subscription_packet")):
            try:
                pkt = feed.create_subscription_packet(instruments)
                logger.info("feed.create_subscription_packet(...) -> %s", pkt)
            except Exception as e:
                logger.debug("create_subscription_packet raised: %s", e)
    except Exception:
        logger.exception("Debug header/sub packet generation failed")

    # 4b) If header missing auth, try setting common Authorization header (Bearer)
    try:
        try:
            hdr = None
            if hasattr(feed, "create_header") and callable(getattr(feed, "create_header")):
                try:
                    hdr = feed.create_header()
                except Exception:
                    hdr = None
            if not hdr or not isinstance(hdr, dict) or not any(k.lower().startswith("auth") for k in hdr.keys()):
                forced = {"Authorization": f"Bearer {ACCESS_TOKEN}", "access-token": ACCESS_TOKEN}
                try:
                    setattr(feed, "header", forced)
                    logger.info("Set feed.header to forced Authorization Bearer header")
                except Exception:
                    try:
                        def _forced_create_header():
                            return forced
                        setattr(feed, "create_header", _forced_create_header)
                        logger.info("Overrode feed.create_header to return Authorization Bearer header")
                    except Exception as e:
                        logger.debug("Could not override create_header: %s", e)
        except Exception:
            pass
    except Exception:
        logger.exception("Forced header override failed")

    # 5) Finally call run_forever (no args). If it raises InvalidStatus (HTTP 400) we log and return error
    if hasattr(feed, "run_forever") and callable(getattr(feed, "run_forever")):
        try:
            logger.info("Calling feed.run_forever() (no args)")
            feed.run_forever()
            # when run_forever returns, treat as invoked/stopped
            return True, None
        except ValueError as ve:
            logger.exception("ValueError invoking run_forever: %s", ve)
            return False, ve
        except Exception as e:
            logger.exception("Exception invoking run_forever (likely ws handshake error): %s", e)
            return True, e

    # fallback attempts
    for alt in ("run", "start", "listen"):
        if hasattr(feed, alt) and callable(getattr(feed, alt)):
            try:
                logger.info("Attempting feed.%s() (no args) as fallback", alt)
                getattr(feed, alt)()
                return True, None
            except Exception as e:
                logger.exception("Exception invoking %s: %s", alt, e)
                return True, e

    logger.error("No runnable entrypoint found on feed instance.")
    return False, None

# -----------------
# Main start logic
# -----------------
def start_market_feed():
    if not CLIENT_ID or not ACCESS_TOKEN:
        logger.error("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN.")
        return

    try:
        import dhanhq as dh
    except Exception as e:
        logger.exception("Failed to import dhanhq: %s", e)
        return

    logger.info("dhanhq module contents: %s", ", ".join(dir(dh)))
    module_obj = getattr(dh, "marketfeed", None) or dh

    # detect feed class
    feed_class = None
    for candidate in ("DhanFeed", "MarketFeed", "DhanMarketFeed"):
        if hasattr(module_obj, candidate):
            feed_class = getattr(module_obj, candidate)
            logger.info("Detected feed class: %s", candidate)
            break
    if not feed_class:
        for candidate in ("DhanFeed", "MarketFeed"):
            if hasattr(dh, candidate):
                feed_class = getattr(dh, candidate)
                module_obj = dh
                logger.info("Detected root-level feed class: %s", candidate)
                break

    # detect constants
    NSE = getattr(module_obj, "NSE", getattr(dh, "NSE", None))
    TICKER = getattr(module_obj, "Ticker", getattr(module_obj, "TICKER", getattr(dh, "Ticker", getattr(dh, "TICKER", None))))
    if NSE is None or TICKER is None:
        instruments = [("NSE", HDFC_ID, "TICKER")]
    else:
        instruments = [(NSE, HDFC_ID, TICKER)]

    logger.info("Instruments to subscribe: %s", instruments)

    version_candidates = [None, "1", "v1", "v2", "2.0"]
    backoff = INITIAL_BACKOFF
    while True:
        try:
            feed = None
            used_version = None
            if feed_class:
                feed, used_version = instantiate_feed_simple(feed_class, CLIENT_ID, ACCESS_TOKEN, instruments, version_candidates)
                if not feed:
                    logger.error("Could not instantiate feed_class for any version candidate.")
                    logger.info("module_obj dir: %s", ", ".join(dir(module_obj)))
                    return
                logger.info("Feed instance created (used_version=%s). type=%s", used_version, type(feed))
            else:
                if hasattr(module_obj, "market_feed_wss"):
                    try:
                        logger.info("Trying module_obj.market_feed_wss(...) fallback")
                        try:
                            module_obj.market_feed_wss(CLIENT_ID, ACCESS_TOKEN, instruments, market_feed_handler)
                        except TypeError:
                            module_obj.market_feed_wss(CLIENT_ID, ACCESS_TOKEN, instruments, callback=market_feed_handler)
                        logger.info("module.market_feed_wss invoked (may block).")
                        time.sleep(1)
                        continue
                    except Exception as e:
                        logger.exception("market_feed_wss invocation failed: %s", e)
                logger.error("No feed_class and no working module-level fallback.")
                return

            started, err = try_start_feed_instance(feed, instruments)
            if started:
                if isinstance(err, ValueError) and "Unsupported version" in str(err):
                    logger.warning("Unsupported version detected after starting attempt; will retry with other versions.")
                    try:
                        if hasattr(feed, "disconnect"):
                            feed.disconnect()
                        elif hasattr(feed, "close_connection"):
                            feed.close_connection()
                    except Exception:
                        pass
                    time.sleep(0.5)
                    continue
                backoff = INITIAL_BACKOFF
                time.sleep(1)
                continue
            else:
                logger.error("Could not start feed instance (no run/start method worked). Dumping diagnostic and exiting.")
                logger.info("feed dir: %s", ", ".join(dir(feed)))
                return

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt; exiting.")
            return
        except Exception as e:
            logger.exception("Unexpected loop error: %s", e)
            sleep = min(MAX_BACKOFF, backoff)
            logger.info("Reconnecting in %.1f s", sleep)
            time.sleep(sleep)
            backoff = min(MAX_BACKOFF, backoff * 2 if backoff > 0 else INITIAL_BACKOFF)

# -----------------
# Signals
# -----------------
def _signal_handler(sig, frame):
    logger.info("Signal %s received; exiting.", sig)
    raise SystemExit()

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# -----------------
# Main
# -----------------
if __name__ == "__main__":
    logger.info("Starting auto-detect DhanHQ bot; HDFC_ID=%s", HDFC_ID)
    start_market_feed()
    logger.info("Bot finished.")
