import os
import io
import sys
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

# ==============================
# ENV VARIABLES (Railway Safe)
# ==============================

SESSION_KEY = os.getenv("DEFINEDGE_SESSION_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not SESSION_KEY:
    print("❌ DEFINEDGE_SESSION_KEY missing")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {SESSION_KEY}"
}

# ==============================
# TELEGRAM
# ==============================

def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠ Telegram not configured")
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print("Telegram Error:", e)


# ==============================
# LOAD MASTER FILE
# ==============================

def load_master_file():

    print("Downloading NSE Cash master file...")

    import requests
    import zipfile
    import io
    import os

    session_key = os.getenv("DEFINEDGE_SESSION_KEY")

    if not session_key:
        print("DEFINEDGE_SESSION_KEY missing.")
        return None

    url = "https://app.definedgesecurities.com/public/nsecash.zip"

    headers = {
        "Authorization": session_key,
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            print(f"Master file HTTP error: {response.status_code}")
            return None

        z = zipfile.ZipFile(io.BytesIO(response.content))
        file_name = z.namelist()[0]

        df = pd.read_csv(z.open(file_name), header=None)

        df.columns = [
            "SEGMENT",
            "TOKEN",
            "SYMBOL",
            "TRADINGSYM",
            "INSTRUMENTTYPE",
            "EXPIRY",
            "TICKSIZE",
            "LOTSIZE",
            "OPTIONTYPE",
            "STRIKE",
            "PRICEPREC",
            "MULTIPLIER",
            "ISIN",
            "PRICEMULT",
            "COMPANY"
        ]

        print("Master file loaded. Rows:", len(df))
        return df

    except Exception as e:
        print("Master file load failed:", e)
        return None



# ==============================
# FETCH HISTORY
# ==============================

def fetch_history(segment, token, timeframe="day"):

    try:
        today = datetime.now()
        from_date = (today - timedelta(days=120)).strftime("%d%m%Y%H%M")
        to_date = today.strftime("%d%m%Y%H%M")

        if timeframe == "day":
            url = f"https://data.definedgesecurities.com/sds/history/{segment}/{token}/{from_date}/{to_date}"
        else:
            url = f"https://data.definedgesecurities.com/sds/history/{segment}/{token}/minute/{from_date}/{to_date}"

        response = requests.get(url, headers=HEADERS, timeout=15)

        if response.status_code != 200:
            return None

        if not response.text.strip():
            return None

        df = pd.read_csv(io.StringIO(response.text), header=None)

        if df.empty:
            return None

        df.columns = ["DATETIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]

        df["DATETIME"] = pd.to_datetime(df["DATETIME"], format="%d%m%Y%H%M", errors="coerce")
        df.dropna(inplace=True)

        return df

    except:
        return None


# ==============================
# TD SEQUENTIAL (SETUP + COUNTDOWN)
# ==============================

def td_sequential(df):

    df = df.copy()
    df["Setup"] = 0
    df["Countdown"] = 0
    df["Direction"] = None

    setup_count = 0
    countdown = 0
    direction = None

    for i in range(4, len(df)):

        if df["CLOSE"].iloc[i] > df["CLOSE"].iloc[i - 4]:
            if direction != "bearish":
                setup_count = 0
            direction = "bearish"
            setup_count += 1

        elif df["CLOSE"].iloc[i] < df["CLOSE"].iloc[i - 4]:
            if direction != "bullish":
                setup_count = 0
            direction = "bullish"
            setup_count += 1
        else:
            setup_count = 0

        df.loc[df.index[i], "Setup"] = setup_count
        df.loc[df.index[i], "Direction"] = direction

        # Countdown logic after setup 9
        if setup_count == 9:
            countdown = 1
        elif countdown > 0:
            countdown += 1

        df.loc[df.index[i], "Countdown"] = countdown

    return df


# ==============================
# MAIN SCANNER
# ==============================

def run():

    print("Starting TD Sequential Scanner...")

    master_df = load_master_file()
    if master_df is None:
        print("Master unavailable. Exiting.")
        return

    master_df = master_df[master_df["SEGMENT"] == "EQ"]
    print(master_df["INSTRUMENTTYPE"].unique())
    print(master_df["SEGMENT"].unique())

    print("Columns:", master_df.columns.tolist())
    print(master_df.head(3))

    print(f"Filtered NSE Cash universe: {len(master_df)}")

    signals_found = 0

    for _, row in tqdm(master_df.iterrows(), total=len(master_df)):

        symbol = row["SYMBOL"]
        token = row["TOKEN"]

        daily_df = fetch_history("NSE", token, "day")
        if daily_df is None:
            continue

        # Volume filter
        if daily_df["VOLUME"].tail(5).mean() < 100000:
            continue

        daily_df = td_sequential(daily_df)

        last = daily_df.iloc[-1]
        setup = last["Setup"]
        countdown = last["Countdown"]
        direction = last["Direction"]

        signal_type = None

        if setup in [7, 8]:
            signal_type = "EARLY"
        elif setup == 9:
            signal_type = "ACTIVE"
        elif countdown >= 13:
            signal_type = "COUNTDOWN 13"

        if signal_type:

            bias = "SELL" if direction == "bearish" else "BUY"

            message = (
                f"<b>📊 TD SIGNAL</b>\n\n"
                f"<b>Stock:</b> {symbol}\n"
                f"<b>Signal:</b> {signal_type}\n"
                f"<b>Bias:</b> {bias}\n"
                f"<b>Setup:</b> {setup}\n"
                f"<b>Countdown:</b> {countdown}\n"
                f"<b>Timeframe:</b> DAILY"
            )

            # Minute confirmation for strong signals
            if signal_type in ["ACTIVE", "COUNTDOWN 13"]:

                minute_df = fetch_history("NSE", token, "minute")

                if minute_df is not None:
                    minute_df = td_sequential(minute_df)
                    min_setup = minute_df.iloc[-1]["Setup"]

                    if min_setup >= 7:
                        message += "\n\n🔥 60min Alignment Confirmed"

            send_telegram(message)
            signals_found += 1

    print(f"Scan Completed. Signals Found: {signals_found}")


# ==============================

if __name__ == "__main__":
    run()
