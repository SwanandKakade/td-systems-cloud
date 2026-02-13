import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import zipfile
import io

# ===============================
# CONFIG
# ===============================

MASTER_URL = "https://app.definedgesecurities.com/public/nsefno.zip"
BASE_HISTORY_URL = "https://data.definedgesecurities.com/sds/history"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SESSION_KEY = os.getenv("DEFINEDGE_SESSION_KEY")

# ===============================
# TELEGRAM
# ===============================

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    requests.post(url, data=payload)


def load_master_file():

    print("Downloading NFO master file...")

    import zipfile
    from io import BytesIO

    url = "https://app.definedgesecurities.com/public/nsefno.zip"

    response = requests.get(url)
    z = zipfile.ZipFile(BytesIO(response.content))

    # Get first file inside zip
    file_name = z.namelist()[0]

    # IMPORTANT: header=None
    df = pd.read_csv(z.open(file_name), header=None)

    # Manually assign column names
    df.columns = [
        "SEGMENT",
        "TOKEN",
        "SYMBOL",
        "TRADINGSYM",
        "INSTRUMENT TYPE",
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


# ===============================
# MASTER FILE (F&O ONLY)
# ===============================
def get_fno_universe():

    df = load_master_file()

    # Make sure correct column names
    df.columns = [col.strip().upper() for col in df.columns]

    # Filter Futures only
    df = df[df["INSTRUMENT TYPE"].isin(["FUTSTK", "FUTIDX"])]

    # Keep only first occurrence per symbol
    df = df.drop_duplicates(subset=["SYMBOL"])

    universe = []

    for _, row in df.iterrows():
        symbol = row["SYMBOL"]
        token = row["TOKEN"]
        universe.append((symbol, token))

    print("Filtered F&O universe:", len(universe))

    return universe




# ===============================
# FETCH MINUTE HISTORY
# ===============================

def fetch_minute_history(segment, token, compression=60):
    end = datetime.today()
    start = end - timedelta(days=120)

    url = f"https://data.definedgesecurities.com/sds/minute/{segment}/{token}/{compression}/{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}"

    print("HISTORY URL:", url)

    headers = {
        "Authorization": SESSION_KEY
    }

    response = requests.get(url, headers=headers, timeout=20)

    if response.status_code != 200:
        print("History fetch failed:", response.status_code)
        return None

    df = pd.read_csv(io.StringIO(response.text))
    return df




# ===============================
# RESAMPLE FUNCTION
# ===============================

def resample_tf(df, rule):
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df.set_index("DateTime", inplace=True)

    df_tf = df.resample(rule).agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum"
    }).dropna()

    return df_tf


# ===============================
# TD SETUP + COUNTDOWN
# ===============================

def td_sequential(df):

    df["TD_Buy_Setup"] = 0
    df["TD_Sell_Setup"] = 0
    df["TD_Buy_Count"] = 0
    df["TD_Sell_Count"] = 0

    buy_setup = 0
    sell_setup = 0
    buy_count = 0
    sell_count = 0

    for i in range(4, len(df)):

        # Setup
        if df["Close"].iloc[i] < df["Close"].iloc[i-4]:
            buy_setup += 1
        else:
            buy_setup = 0

        if df["Close"].iloc[i] > df["Close"].iloc[i-4]:
            sell_setup += 1
        else:
            sell_setup = 0

        df.iloc[i, df.columns.get_loc("TD_Buy_Setup")] = buy_setup
        df.iloc[i, df.columns.get_loc("TD_Sell_Setup")] = sell_setup

        # Countdown starts only after setup 9
        if buy_setup >= 9:
            if df["Close"].iloc[i] <= df["Low"].iloc[i-2]:
                buy_count += 1
        else:
            buy_count = 0

        if sell_setup >= 9:
            if df["Close"].iloc[i] >= df["High"].iloc[i-2]:
                sell_count += 1
        else:
            sell_count = 0

        df.iloc[i, df.columns.get_loc("TD_Buy_Count")] = buy_count
        df.iloc[i, df.columns.get_loc("TD_Sell_Count")] = sell_count

    return df


# ===============================
# SIGNAL CHECK
# ===============================

def get_signal(df):
    last = df.iloc[-1]

    if last["TD_Buy_Count"] == 13:
        return "BUY_13"
    if last["TD_Sell_Count"] == 13:
        return "SELL_13"

    if last["TD_Buy_Setup"] == 9:
        return "BUY_9"
    if last["TD_Sell_Setup"] == 9:
        return "SELL_9"

    return None


# ===============================
# DAILY TREND
# ===============================

def get_daily_trend(df):
    df["EMA20"] = df["Close"].ewm(span=20).mean()
    last = df.iloc[-1]

    if last["Close"] > last["EMA20"]:
        return "UPTREND"
    return "DOWNTREND"


# ===============================
# MAIN RUNNER
# ===============================

def run():

    universe = get_fno_universe()

    for symbol, token in universe:

        print("Scanning:", symbol)

        df = fetch_minute_history("NFO", token)
        if df is None or len(df) < 100:
            continue

        df_60m = td_sequential(resample_tf(df.copy(), "60min"))
        df_2h = td_sequential(resample_tf(df.copy(), "120min"))
        df_day = resample_tf(df.copy(), "1D")

        signal = get_signal(df_60m)

        if not signal:
            continue

        confirm_2h = get_signal(df_2h)
        trend = get_daily_trend(df_day)

        message = f"""
🔥 TD SEQUENTIAL ALERT

Stock: {symbol}
Primary (60m): {signal}
2H Confirm: {confirm_2h if confirm_2h else "No"}
Daily Trend: {trend}
"""

        print(message)
        send_telegram(message)


# ===============================
# ENTRY POINT
# ===============================

if __name__ == "__main__":
    print("Starting TD Sequential Scanner...")
    run()
