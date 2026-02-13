import os
import io
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta

SESSION_KEY = os.getenv("DEFINEDGE_SESSION_KEY")

BASE_HISTORY_URL = "https://data.definedgesecurities.com/sds/history"
MASTER_URL = "https://app.definedgesecurities.com/public/nsecm.zip"


# =====================================================
# MASTER FILE (LOAD ONCE)
# =====================================================
def load_master_file():
    print("Downloading NSE Cash master file...")

    response = requests.get(MASTER_URL)
    z = zipfile.ZipFile(io.BytesIO(response.content))

    file_name = z.namelist()[0]

    df = pd.read_csv(z.open(file_name), header=None)

    df.columns = [
        "SEGMENT", "TOKEN", "SYMBOL", "TRADINGSYM", "SERIES",
        "TICKSIZE", "LOTSIZE", "ISIN",
        "PRICEPREC", "MULTIPLIER", "PRICEMULT", "COMPANY"
    ]

    print("Master file loaded. Rows:", len(df))
    return df


def get_nse_cash_universe(master_df):
    df = master_df.copy()
    df.columns = df.columns.str.strip().str.upper()

    df = df[(df["SEGMENT"] == "NSE") & (df["SERIES"] == "EQ")]

    universe = list(zip(df["SYMBOL"], df["TOKEN"]))

    print("Filtered NSE Cash universe:", len(universe))
    return universe


# =====================================================
# SAFE HISTORY FETCH
# =====================================================
def fetch_history(segment, token, timeframe="day"):

    end = datetime.now()
    start = end - timedelta(days=180)

    from_date = start.strftime("%d%m%Y") + "0915"
    to_date   = end.strftime("%d%m%Y") + "1530"

    url = f"{BASE_HISTORY_URL}/{segment}/{token}/{timeframe}/{from_date}/{to_date}"

    headers = {"Authorization": SESSION_KEY}

    try:
        response = requests.get(url, headers=headers, timeout=20)
    except:
        return None

    if response.status_code != 200:
        return None

    if not response.text.strip():
        return None

    try:
        df = pd.read_csv(io.StringIO(response.text), header=None)
    except:
        return None

    if df.empty:
        return None

    df.columns = ["DATETIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
    df["DATETIME"] = pd.to_datetime(df["DATETIME"], format="%d%m%Y%H%M")

    return df


# =====================================================
# RESAMPLE
# =====================================================
def resample_tf(df, tf="60min"):

    df = df.copy()
    df.set_index("DATETIME", inplace=True)

    df = df.resample(tf).agg({
        "OPEN": "first",
        "HIGH": "max",
        "LOW": "min",
        "CLOSE": "last",
        "VOLUME": "sum"
    }).dropna()

    df.reset_index(inplace=True)
    return df


# =====================================================
# TD SEQUENTIAL LOGIC
# =====================================================
def td_sequential(df):

    df = df.copy()
    df["TD_SETUP"] = 0
    df["TD_COUNTDOWN"] = 0

    setup_count = 0
    countdown = 0

    for i in range(4, len(df)):

        # BUY setup
        if df["CLOSE"].iloc[i] < df["CLOSE"].iloc[i-4]:
            setup_count += 1
        else:
            setup_count = 0

        df.loc[df.index[i], "TD_SETUP"] = setup_count

        # Countdown (simplified version)
        if setup_count >= 9:
            if df["CLOSE"].iloc[i] <= df["LOW"].iloc[i-2]:
                countdown += 1
            df.loc[df.index[i], "TD_COUNTDOWN"] = countdown

    return df


# =====================================================
# VOLUME FILTER
# =====================================================
def passes_volume_filter(df, min_avg_volume=500000):

    avg_vol = df["VOLUME"].tail(20).mean()

    return avg_vol >= min_avg_volume


# =====================================================
# DAILY TD FILTER (7/8/9)
# =====================================================
def near_td_setup(df):

    df = td_sequential(df)

    if len(df) < 10:
        return False

    last = df.iloc[-1]

    return last["TD_SETUP"] in [7, 8, 9]


# =====================================================
# MAIN RUN
# =====================================================
def run():

    print("Starting TD Sequential Scanner...")

    master_df = load_master_file()
    universe = get_nse_cash_universe(master_df)

    total = len(universe)
    shortlist = []

    print("\n--- DAILY SCAN START ---\n")

    for i, (symbol, token) in enumerate(universe, start=1):

        print(f"[{i}/{total}] Daily scan: {symbol}")

        df_daily = fetch_history("NSE", token, "day")

        if df_daily is None:
            continue

        if not passes_volume_filter(df_daily):
            continue

        if not near_td_setup(df_daily):
            continue

        shortlist.append((symbol, token))

        time.sleep(0.2)

    print("\nDaily shortlist:", len(shortlist))

    print("\n--- 60 MIN SCAN START ---\n")

    for i, (symbol, token) in enumerate(shortlist, start=1):

        print(f"[{i}/{len(shortlist)}] 60m scan: {symbol}")

        df_min = fetch_history("NSE", token, "minute")

        if df_min is None:
            continue

        df_60m = resample_tf(df_min, "60min")
        df_60m = td_sequential(df_60m)

        last = df_60m.iloc[-1]

        if last["TD_SETUP"] == 9:
            print(f"TD9 ACTIVE → {symbol}")

        if last["TD_COUNTDOWN"] >= 13:
            print(f"TD13 COMPLETE → {symbol}")

        time.sleep(0.2)


# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    run()
