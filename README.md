# DhanHQ WebSocket Alert Bot

This package contains a robust DhanHQ WebSocket alert bot that:
- auto-detects available dhanhq API shapes,
- tries multiple constructor signatures and versions,
- registers callbacks, authorizes, subscribes, then runs the feed,
- sends throttled Telegram alerts for LTP updates.

Files:
- dhan_websocket_alert_bot.py : main bot
- requirements.txt : dependencies
- Procfile : Railway process command

Deployment:
1. Set environment variables in Railway:
   - DHAN_CLIENT_ID
   - DHAN_ACCESS_TOKEN
   - TELEGRAM_BOT_TOKEN
   - TELEGRAM_CHAT_ID
   - (optional) HDFC_ID, SEND_INTERVAL_SECONDS
2. Push this repo to GitHub and connect to Railway or upload files directly.
3. Railway will run the Procfile command.

Notes:
- If you hit WebSocket HTTP 400, check logs for feed.create_header() and create_subscription_packet() output.
- Paste logs into the chat and I will help debug further.
