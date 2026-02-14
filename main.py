import os
import requests
import pandas as pd
import zipfile
import io
import logging
import numpy as np
from datetime import datetime, timedelta
from demark_engine import DeMarkEngine

# ---------------- SAFE TQDM ---------------- #
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

# ---------------- HISTORY FUNCTIONS ---------------- #

def fetch_daily(token):
    try:
        end = datetime.now()
        start = end - timedelta(days=200)

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
        df["DATETIME"] = pd.to_datetime(df["DATETIME"])
        df = df.sort_values("DATETIME")

        return df

    except:
        return None

# ---------------- SIGNAL AGE CLASSIFIER ---------------- #

def classify_age(age):
    if age is None:
        return "Neutral"
    if age <= 3:
        return "Fresh"
    if age <= 8:
        return "Active"
    return "Expired"

# ---------------- RUN ---------------- #

def run():

    logging.info("Starting TD Institutional Scanner...")

    master = load_master_file()
    if master is None:
        logging.error("Master unavailable.")
        return

    table_rows = []
    total_scanned = 0

    for _, row in tqdm(master.iterrows(), total=len(master)):

        token = row["TOKEN"]
        symbol = row["SYMBOL"]

        total_scanned += 1

        try:
            # =======================
            # DAILY TIMEFRAME
            # =======================
            df_daily = fetch_daily(token)
            if df_daily is None or len(df_daily) < 200:
                continue

            if df_daily["VOLUME"].tail(20).mean() < 100000:
                continue

            # 200 EMA
            df_daily["EMA200"] = df_daily["CLOSE"].ewm(span=200).mean()
            last_daily_close = df_daily["CLOSE"].iloc[-1]
            last_ema200 = df_daily["EMA200"].iloc[-1]

            daily_trend_bull = last_daily_close > last_ema200
            daily_trend_bear = last_daily_close < last_ema200

            # =======================
            # 1H TIMEFRAME
            # =======================
            df_1h = fetch_1h(token)
            if df_1h is None or len(df_1h) < 50:
                continue

            engine_1h = DeMarkEngine(df_1h)
            h1 = engine_1h.run()
            last_1h = h1.iloc[-1]

            # =======================
            # CONFIDENCE SCORING
            # =======================
            confidence = 0
            final_signal = None
            signal_age = None

            # BUY
            if daily_trend_bull and last_1h["valid_buy_13"]:
                final_signal = "Buy Exhaustion"
                confidence += 3

            if daily_trend_bull and last_1h["perfect_buy"]:
                confidence += 1

            # SELL
            if daily_trend_bear and last_1h["valid_sell_13"]:
                final_signal = "Sell Exhaustion"
                confidence += 3

            if daily_trend_bear and last_1h["perfect_sell"]:
                confidence += 1

            if final_signal is None:
                continue

            # =======================
            # SIGNAL AGING
            # =======================
            if final_signal == "Buy Exhaustion":
                valid_indices = h1.index[h1["valid_buy_13"] == True]
            else:
                valid_indices = h1.index[h1["valid_sell_13"] == True]

            if len(valid_indices) > 0:
                last_signal_index = valid_indices[-1]
                age = len(h1) - h1.index.get_loc(last_signal_index) - 1
                signal_age = classify_age(age)
            else:
                signal_age = "Fresh"

            # =======================
            # BUILD ROW
            # =======================
            table_rows.append({
                "symbol": symbol,
                "h1": "Yes",
                "daily": "Bull" if daily_trend_bull else "Bear",
                "signal": final_signal,
                "status": signal_age,
                "confidence": confidence
            })

        except Exception as e:
            logging.warning(f"Error processing {symbol}: {e}")
            continue

    # =======================
    # TELEGRAM OUTPUT
    # =======================

    logging.info(f"Scan Completed. Symbols scanned: {total_scanned}")

    if not table_rows:
        logging.info("No aligned institutional signals.")
        return

    message = "📊 TD Institutional Scanner\n\n"
    message += "Symbol | 1H | Daily | Signal | Status | Conf\n"
    message += "------------------------------------------------\n"

    for row in table_rows[:25]:
        message += f"{row['symbol']} | {row['h1']} | {row['daily']} | {row['signal']} | {row['status']} | {row['confidence']}\n"

    send_telegram(message)
    logging.info("Telegram alert sent.")


if __name__ == "__main__":
    run()
