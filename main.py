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

# ---------------- MASTER ---------------- #

def load_master_file():
    try:
        response = requests.get(MASTER_URL)
        if response.status_code != 200:
            return None

        z = zipfile.ZipFile(io.BytesIO(response.content))
        df = pd.read_csv(z.open(z.namelist()[0]))

        df.columns = [
            "EXCHANGE","TOKEN","SYMBOL","TRADINGSYM",
            "INSTRUMENTTYPE","EXPIRY","TICKSIZE","LOTSIZE",
            "OPTIONTYPE","STRIKE","PRICEPREC","MULTIPLIER",
            "ISIN","PRICEMULT","COMPANY"
        ]

        return df[df["INSTRUMENTTYPE"] == "EQ"]

    except:
        return None

# ---------------- DATA FETCH ---------------- #

def fetch_daily(token):
    try:
        end = datetime.now()
        start = end - timedelta(days=150)

        url = f"https://data.definedgesecurities.com/sds/history/NSE/{token}/day/{start.strftime('%d%m%Y')}0000/{end.strftime('%d%m%Y')}2359"
        headers = {"Authorization": DEFINEDGE_SESSION}

        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            return None

        df = pd.read_csv(io.StringIO(r.text))
        if df.empty:
            return None

        df.columns = ["DATETIME","OPEN","HIGH","LOW","CLOSE","VOLUME"]
        df = df.sort_values("DATETIME")

        df.rename(columns={
            "OPEN":"open",
            "HIGH":"high",
            "LOW":"low",
            "CLOSE":"close",
            "VOLUME":"volume"
        }, inplace=True)

        return df

    except:
        return None


def fetch_1h(token):
    try:
        end = datetime.now()
        start = end - timedelta(days=30)

        url = f"https://data.definedgesecurities.com/sds/history/NSE/{token}/60/{start.strftime('%d%m%Y')}0000/{end.strftime('%d%m%Y')}2359"
        headers = {"Authorization": DEFINEDGE_SESSION}

        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            return None

        df = pd.read_csv(io.StringIO(r.text))
        if df.empty:
            return None

        df.columns = ["DATETIME","OPEN","HIGH","LOW","CLOSE","VOLUME"]
        df = df.sort_values("DATETIME")

        df.rename(columns={
            "OPEN":"open",
            "HIGH":"high",
            "LOW":"low",
            "CLOSE":"close",
            "VOLUME":"volume"
        }, inplace=True)

        return df

    except:
        return None

# ---------------- CLASSIFICATION ---------------- #

def classify_signal(daily, h1):

    classification = None
    confidence = 0

    daily_bias = daily["close"] > daily["EMA200"]

    # ---------------- RULES ---------------- #

    # Fresh Buy
    if daily_bias and h1["BUY_STATUS"] in ["Fresh","Active"]:
        classification = "Fresh Buy"
        confidence += 2

    # Strong Sell
    if daily["VALID_SELL_13"] and h1["VALID_SELL_13"]:
        classification = "Strong Sell"
        confidence += 2

    # Early Exhaustion
    if daily["buy_setup"] >= 9 and not daily["VALID_BUY_13"]:
        classification = "Early Buy Exhaustion"

    if daily["sell_setup"] >= 9 and not daily["VALID_SELL_13"]:
        classification = "Early Sell Exhaustion"

    # Intraday Exhaustion
    if h1["buy_setup"] >= 9 and not daily["VALID_BUY_13"]:
        classification = "Intraday Buy Exhaustion"

    if h1["sell_setup"] >= 9 and not daily["VALID_SELL_13"]:
        classification = "Intraday Sell Exhaustion"

    # ---------------- CONFIDENCE ---------------- #

    if daily["VALID_BUY_13"]:
        confidence += 2

    if daily["perfect_buy"]:
        confidence += 1

    if h1["VALID_BUY_13"]:
        confidence += 1

    if daily_bias:
        confidence += 1

    if h1["BUY_AGE"] <= 3:
        confidence += 1

    return classification, confidence


# ---------------- RUN ---------------- #

def run():

    logging.info("Starting TD Framework v1.0 Scanner...")

    master = load_master_file()
    if master is None:
        logging.info("Master unavailable.")
        return

    rows = []
    scanned = 0

    for _, row in tqdm(master.iterrows(), total=len(master)):

        token = row["TOKEN"]
        symbol = row["SYMBOL"]
        scanned += 1

        df_daily = fetch_daily(token)
        if df_daily is None or len(df_daily) < 50:
            continue

        engine_daily = DeMarkEngine(df_daily)
        daily = engine_daily.run()
        daily_last = daily.iloc[-1]

        df_1h = fetch_1h(token)
        if df_1h is None or len(df_1h) < 50:
            continue

        engine_1h = DeMarkEngine(df_1h)
        h1 = engine_1h.run()
        h1_last = h1.iloc[-1]

        classification, confidence = classify_signal(daily_last, h1_last)

        if classification and confidence >= 4:

            rows.append(
                f"{symbol:<12} | "
                f"1H:{h1_last['BUY_STATUS']}/{h1_last['SELL_STATUS']} | "
                f"D:{daily_last['BUY_STATUS']}/{daily_last['SELL_STATUS']} | "
                f"{classification} | "
                f"Conf:{confidence}"
            )

    logging.info(f"Scanned: {scanned}")
    logging.info(f"Qualified Signals: {len(rows)}")

    if not rows:
        return

    message = (
        "📊 Signals\n\n"
        + "\n".join(rows[:40])
    )

    send_telegram(message)


if __name__ == "__main__":
    run()
