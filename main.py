import os
import requests
import pandas as pd
import zipfile
import io
import logging
from datetime import datetime, timedelta

# Safe tqdm import (Railway safe)
try:
    from tqdm import tqdm
except:
    def tqdm(x, **kwargs):
        return x

# ---------------- CONFIG ---------------- #

DEFINEDGE_SESSION = os.getenv("DEFINEDGE_SESSION_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

MASTER_URL = "https://app.definedgesecurities.com/public/nsecash.zip"

logging.basicConfig(level=logging.INFO, format="%(message)s")

# ---------------- TELEGRAM ---------------- #

def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": message})
    except:
        pass

# ---------------- MASTER FILE ---------------- #

def load_master_file():
    try:
        response = requests.get(MASTER_URL)
        if response.status_code != 200:
            logging.info(f"Master HTTP error: {response.status_code}")
            return None

        z = zipfile.ZipFile(io.BytesIO(response.content))
        csv_file = z.namelist()[0]
        df = pd.read_csv(z.open(csv_file))

        # Assign correct columns
        df.columns = [
            "EXCHANGE","TOKEN","SYMBOL","TRADINGSYM",
            "INSTRUMENTTYPE","EXPIRY","TICKSIZE","LOTSIZE",
            "OPTIONTYPE","STRIKE","PRICEPREC","MULTIPLIER",
            "ISIN","PRICEMULT","COMPANY"
        ]

        df = df[df["INSTRUMENTTYPE"] == "EQ"]
        return df

    except Exception as e:
        logging.info(f"Master load failed: {e}")
        return None

# ---------------- HISTORY ---------------- #

def fetch_daily(token):
    try:
        end = datetime.now()
        start = end - timedelta(days=120)

        url = f"https://data.definedgesecurities.com/sds/history/NSE/{token}/day/{start.strftime('%d%m%Y')}0000/{end.strftime('%d%m%Y')}2359"

        headers = {"Authorization": DEFINEDGE_SESSION}

        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            return None

        df = pd.read_csv(io.StringIO(r.text))

        if df.empty:
            return None

        df.columns = ["DATETIME","OPEN","HIGH","LOW","CLOSE","VOLUME"]
        df["DATETIME"] = pd.to_datetime(df["DATETIME"])
        df = df.sort_values("DATETIME")
        return df

    except:
        return None

# ---------------- TD LOGIC ---------------- #

def td_setup(df):

    df["bull_setup"] = 0
    df["bear_setup"] = 0

    for i in range(4, len(df)):
        if df["CLOSE"].iloc[i] < df["CLOSE"].iloc[i-4]:
            df.loc[df.index[i], "bull_setup"] = df["bull_setup"].iloc[i-1] + 1
        else:
            df.loc[df.index[i], "bull_setup"] = 0

        if df["CLOSE"].iloc[i] > df["CLOSE"].iloc[i-4]:
            df.loc[df.index[i], "bear_setup"] = df["bear_setup"].iloc[i-1] + 1
        else:
            df.loc[df.index[i], "bear_setup"] = 0

    return df

def td_countdown(df):

    df["bull_countdown"] = 0
    df["bear_countdown"] = 0

    bull_cd = 0
    bear_cd = 0

    for i in range(2, len(df)):

        if df["bull_setup"].iloc[i] >= 9:
            bull_cd = 0

        if df["bear_setup"].iloc[i] >= 9:
            bear_cd = 0

        # Bullish countdown
        if df["CLOSE"].iloc[i] <= df["LOW"].iloc[i-2]:
            bull_cd += 1
            df.loc[df.index[i], "bull_countdown"] = bull_cd

        # Bearish countdown
        if df["CLOSE"].iloc[i] >= df["HIGH"].iloc[i-2]:
            bear_cd += 1
            df.loc[df.index[i], "bear_countdown"] = bear_cd

    return df

# ---------------- RUN ---------------- #

def run():

    logging.info("Starting TD Sequential Scanner...")

    master = load_master_file()
    if master is None:
        logging.info("Master unavailable.")
        return

    signals = []

    for _, row in tqdm(master.iterrows(), total=len(master)):

        token = row["TOKEN"]
        symbol = row["SYMBOL"]

        df = fetch_daily(token)
        if df is None or len(df) < 20:
            continue

        # Volume filter
        if df["VOLUME"].tail(20).mean() < 100000:
            continue

        df = td_setup(df)
        df = td_countdown(df)

        last = df.iloc[-1]

        # Setup density boost
        if last["bull_setup"] in [6,7,8,9]:
            signals.append(f"🟢 {symbol} Bull Setup {int(last['bull_setup'])}")

        if last["bear_setup"] in [6,7,8,9]:
            signals.append(f"🔴 {symbol} Bear Setup {int(last['bear_setup'])}")

        # Countdown alerts
        if last["bull_countdown"] in [10,11,12]:
            signals.append(f"🟢 {symbol} Bull Countdown {int(last['bull_countdown'])}")

        if last["bull_countdown"] == 13:
            signals.append(f"🚀 {symbol} Bullish 13 Exhaustion")

        if last["bear_countdown"] in [10,11,12]:
            signals.append(f"🔴 {symbol} Bear Countdown {int(last['bear_countdown'])}")

        if last["bear_countdown"] == 13:
            signals.append(f"🔥 {symbol} Bearish 13 Exhaustion")

    logging.info(f"Scan Completed. Signals Found: {len(signals)}")

    if signals:
        message = "📊 TD Signals:\n\n" + "\n".join(signals[:40])
        send_telegram(message)


if __name__ == "__main__":
    run()
