import os
import requests
import pandas as pd
import zipfile
import io
import logging
from datetime import datetime, timedelta
from demark_engine import DeMarkEngine

try:
    from tqdm import tqdm
except:
    def tqdm(x, **kwargs):
        return x


# ================= CONFIG ================= #

DEFINEDGE_SESSION = os.getenv("DEFINEDGE_SESSION_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

MASTER_URL = "https://app.definedgesecurities.com/public/nsecash.zip"

# Put correct NIFTY token from master
NIFTY_TOKEN = "26000"

logging.basicConfig(level=logging.INFO, format="%(message)s")


# ================= TELEGRAM ================= #

def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": message})
    except:
        pass


# ================= MASTER ================= #

def load_master_file():
    try:
        r = requests.get(MASTER_URL)
        if r.status_code != 200:
            return None

        z = zipfile.ZipFile(io.BytesIO(r.content))
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


# ================= HISTORY ================= #

def fetch_data(token, timeframe, days):

    try:
        end = datetime.now()
        start = end - timedelta(days=days)

        url = f"https://data.definedgesecurities.com/sds/history/NSE/{token}/{timeframe}/{start.strftime('%d%m%Y')}0000/{end.strftime('%d%m%Y')}2359"

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


# ================= RUN ================= #

def run():

    logging.info("Starting TD Framework v1.2 Scanner...")

    master = load_master_file()
    if master is None:
        logging.error("Master unavailable.")
        return

    # Fetch Nifty once
    nifty_df = fetch_data(NIFTY_TOKEN, "day", 250)
    if nifty_df is None or len(nifty_df) < 100:
        logging.error("Nifty data unavailable.")
        return

    nifty_df = nifty_df.set_index("DATETIME")

    results = []
    total_scanned = 0

    for _, row in tqdm(master.iterrows(), total=len(master)):

        token = row["TOKEN"]
        symbol = row["SYMBOL"]
        total_scanned += 1

        try:

            # ================= DAILY ================= #

            daily_df = fetch_data(token, "day", 250)
            if daily_df is None or len(daily_df) < 150:
                continue

            daily_df["SMA200"] = daily_df["CLOSE"].rolling(200).mean()
            daily_df = daily_df.set_index("DATETIME")

            # Ratio Strength
            merged = daily_df.join(
                nifty_df["CLOSE"],
                how="inner",
                rsuffix="_NIFTY"
            )

            merged["RATIO"] = merged["CLOSE"] / merged["CLOSE_NIFTY"]
            merged["RATIO_EMA"] = merged["RATIO"].ewm(span=20).mean()

            ratio_strong = merged["RATIO"].iloc[-1] > merged["RATIO_EMA"].iloc[-1]

            engine_daily = DeMarkEngine(daily_df.reset_index())
            daily = engine_daily.run()
            d = daily.iloc[-1]

            # Bias
            bullish_bias = daily_df["CLOSE"].iloc[-1] > daily_df["SMA200"].iloc[-1]
            bias = "Bull" if bullish_bias else "Bear"

            # ================= 1H ================= #

            hourly_df = fetch_data(token, "60", 40)
            if hourly_df is None or len(hourly_df) < 80:
                continue

            engine_hour = DeMarkEngine(hourly_df)
            hourly = engine_hour.run()
            h = hourly.iloc[-1]

            # ================= CLASSIFICATION ================= #

            signal = None
            confidence = 0

            # ----- STRONG SELL -----
            if d["td13_sell_status"] in ["Fresh","Active"] and \
               h["td13_sell_status"] in ["Fresh","Active"]:

                signal = "Strong Sell"
                confidence += 2

            # ----- FRESH BUY -----
            elif bullish_bias and \
                 h["td13_buy_status"] in ["Fresh","Active"]:

                signal = "Fresh Buy"
                confidence += 2

            # ----- EARLY EXHAUSTION (Daily TD9) -----
            elif d["td9_buy_status"] in ["Fresh","Active"]:
                signal = "Early Buy Exhaustion"

            elif d["td9_sell_status"] in ["Fresh","Active"]:
                signal = "Early Sell Exhaustion"

            # ----- INTRADAY EXHAUSTION -----
            elif h["td9_buy_status"] in ["Fresh","Active"]:
                signal = "Intraday Buy Exhaustion"

            elif h["td9_sell_status"] in ["Fresh","Active"]:
                signal = "Intraday Sell Exhaustion"

            # ================= CONFIDENCE SCORING ================= #

            if d["td13_buy_status"] in ["Fresh","Active"]:
                confidence += 2

            if h["td13_buy_status"] in ["Fresh","Active"]:
                confidence += 1

            if ratio_strong:
                confidence += 1

            if h["td13_buy_age"] <= 3:
                confidence += 1

            confidence = min(confidence, 6)

            # ================= FILTER ================= #

            if confidence >= 4 and signal:

                leadership = "Leader" if ratio_strong else "Lagging"

                results.append(
                    f"{symbol:<12} | {bias:<4} | "
                    f"D:{d['td13_buy_status'] or d['td13_sell_status']:<8} | "
                    f"H:{h['td13_buy_status'] or h['td13_sell_status']:<8} | "
                    f"{signal:<22} | {confidence}/6 | {leadership}"
                )

        except:
            continue

    logging.info(f"Scanned: {total_scanned}")
    logging.info(f"Signals: {len(results)}")

    if not results:
        logging.info("No high-confidence signals.")
        return

    message = (
        "📊 Signals\n"
        "Stock        |Bias |Daily     |1H        |Signal                |Score|Type\n"
        "--------------------------------------------------------------------------\n"
        + "\n".join(results[:40])
    )

    send_telegram(message)


if __name__ == "__main__":
    run()
