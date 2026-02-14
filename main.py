import os
import requests
import pandas as pd
import zipfile
import io
import logging
from datetime import datetime, timedelta
from demark_engine import DeMarkEngine

# Safe tqdm
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
            return None

        z = zipfile.ZipFile(io.BytesIO(response.content))
        csv_file = z.namelist()[0]
        df = pd.read_csv(z.open(csv_file))

        df.columns = [
            "EXCHANGE","TOKEN","SYMBOL","TRADINGSYM",
            "INSTRUMENTTYPE","EXPIRY","TICKSIZE","LOTSIZE",
            "OPTIONTYPE","STRIKE","PRICEPREC","MULTIPLIER",
            "ISIN","PRICEMULT","COMPANY"
        ]

        df = df[df["INSTRUMENTTYPE"] == "EQ"]
        return df

    except:
        return None

# ---------------- DATA FETCH ---------------- #

def fetch_daily(token):
    try:
        end = datetime.now()
        start = end - timedelta(days=180)

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


def fetch_1h(token):
    try:
        end = datetime.now()
        start = end - timedelta(days=60)

        url = f"https://data.definedgesecurities.com/sds/history/NSE/{token}/60/{start.strftime('%d%m%Y')}0000/{end.strftime('%d%m%Y')}2359"
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

# ---------------- RUN ENGINE ---------------- #

def run():

    logging.info("Starting TD Framework v1.0 Scanner...")

    master = load_master_file()
    if master is None:
        logging.info("Master unavailable.")
        return

    results = []
    total = 0

    for _, row in tqdm(master.iterrows(), total=len(master)):

        token = row["TOKEN"]
        symbol = row["SYMBOL"]
        total += 1

        try:
            # ======================
            # DAILY ENGINE
            # ======================

            df_daily = fetch_daily(token)
            if df_daily is None or len(df_daily) < 50:
                continue

            if df_daily["VOLUME"].tail(20).mean() < 100000:
                continue

            engine_daily = DeMarkEngine(df_daily)
            daily = engine_daily.run()
            d_last = daily.iloc[-1]

            # 200 EMA Bias
            bias = d_last["BIAS"]  # Bullish / Bearish

            # ======================
            # 1H ENGINE
            # ======================

            df_1h = fetch_1h(token)
            if df_1h is None or len(df_1h) < 50:
                continue

            engine_1h = DeMarkEngine(df_1h)
            h1 = engine_1h.run()
            h_last = h1.iloc[-1]

            # ======================
            # CLASSIFICATION LOGIC
            # ======================

            classification = ""
            confidence = 0

            # Daily TD13
            if d_last["BUY_STATUS"] in ["Fresh", "Active"]:
                confidence += 2
            if d_last["SELL_STATUS"] in ["Fresh", "Active"]:
                confidence += 2

            # 1H TD13
            if h_last["BUY_STATUS"] in ["Fresh", "Active"]:
                confidence += 1
            if h_last["SELL_STATUS"] in ["Fresh", "Active"]:
                confidence += 1

            # Perfect setups
            if d_last["PERFECT_BUY"]:
                confidence += 1
            if d_last["PERFECT_SELL"]:
                confidence += 1

            # Trend alignment
            if bias == "Bullish" and h_last["BUY_STATUS"] in ["Fresh","Active"]:
                confidence += 1
            if bias == "Bearish" and h_last["SELL_STATUS"] in ["Fresh","Active"]:
                confidence += 1

            # Classification rules

            if bias == "Bullish" and h_last["BUY_STATUS"] in ["Fresh","Active"]:
                classification = "Fresh Buy"

            if bias == "Bearish" and h_last["SELL_STATUS"] in ["Fresh","Active"]:
                classification = "Fresh Sell"

            if d_last["BUY_STATUS"] in ["Fresh","Active"] and \
               h_last["BUY_STATUS"] in ["Fresh","Active"]:
                classification = "Strong Buy"

            if d_last["SELL_STATUS"] in ["Fresh","Active"] and \
               h_last["SELL_STATUS"] in ["Fresh","Active"]:
                classification = "Strong Sell"

            if d_last["PERFECT_BUY"] and classification == "":
                classification = "Early Exhaustion"

            if d_last["PERFECT_SELL"] and classification == "":
                classification = "Early Exhaustion"

            if h_last["PERFECT_BUY"] and classification == "":
                classification = "Intraday Exhaustion"

            if h_last["PERFECT_SELL"] and classification == "":
                classification = "Intraday Exhaustion"

            if classification == "":
                continue

            if confidence < 4:
                continue

            results.append(
                f"{symbol:<12} | 1H: {h_last['BUY_STATUS']}/{h_last['SELL_STATUS']} "
                f"| Daily: {d_last['BUY_STATUS']}/{d_last['SELL_STATUS']} "
                f"| {classification} | Score: {confidence}"
            )

        except:
            continue

    logging.info(f"Scan Completed. Scanned: {total}")
    logging.info(f"Signals Found: {len(results)}")

    if not results:
        logging.info("No actionable signals.")
        return

    message = (
        "📊 TD Framework v1.0 Signals\n"
        f"Scanned: {total}\n\n"
        + "\n".join(results[:40])
    )

    send_telegram(message)


if __name__ == "__main__":
    run()
