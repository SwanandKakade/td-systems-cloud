import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# ===============================
# CONFIG (Environment Variables)
# ===============================
DEFINEDGE_API_KEY = os.getenv("DEFINEDGE_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

BASE_URL = "https://data.definedgesecurities.com"

# ===============================
# Telegram Sender
# ===============================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    requests.post(url, data=payload)

# ===============================
# Fetch Historical Data
# ===============================
def fetch_history(segment, token):
    end = datetime.today()
    start = end - timedelta(days=120)

    url = f"{BASE_URL}/sds/history/{segment}/{token}/day/{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}"
    headers = {"Authorization": f"Bearer {DEFINEDGE_API_KEY}"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return None

    df = pd.read_csv(pd.compat.StringIO(response.text))
    return df

# ===============================
# TD Logic
# ===============================
def calculate_td(df):
    df["buy_setup"] = (df["Close"] < df["Close"].shift(4)).astype(int)
    df["sell_setup"] = (df["Close"] > df["Close"].shift(4)).astype(int)

    df["buy_count"] = df["buy_setup"] * (
        df["buy_setup"].groupby((df["buy_setup"] == 0).cumsum()).cumcount() + 1
    )
    df["sell_count"] = df["sell_setup"] * (
        df["sell_setup"].groupby((df["sell_setup"] == 0).cumsum()).cumcount() + 1
    )

    latest = df.iloc[-1]

    signal = None
    if latest["buy_count"] == 9:
        signal = "TD9 BUY"
    elif latest["sell_count"] == 9:
        signal = "TD9 SELL"

    return signal

# ===============================
# MAIN ENGINE
# ===============================
def run():
    # Example test symbol (replace later with full NSE universe loop)
    segment = "NSE"
    token = "TCS"

    df = fetch_history(segment, token)
    if df is None:
        send_telegram("TD Engine: Failed to fetch data.")
        return

    signal = calculate_td(df)

    if signal:
        send_telegram(f"{token} -> {signal}")
    else:
        send_telegram("TD Engine ran. No signals today.")

if __name__ == "__main__":
    run()
