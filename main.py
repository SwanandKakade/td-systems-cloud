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

# ⚠️ Put correct NIFTY token here
NIFTY_TOKEN = "26000"  # <-- Replace with correct token from master file

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

    logging.info("Starting TD Framework v1.1 Scanner...")

    master = load_master_file()
    if master is None:
        logging.error("Master unavailable.")
        return

    # Fetch Nifty once
    nifty_df = fetch_data(NIFTY_TOKEN, "day", 200)
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

            daily_df = fetch_data(token, "day", 200)
            if daily_df is None or len(daily_df) < 100:
                continue

            daily_df["EMA200"] = daily_df["CLOSE"].ewm(span=200).mean()

            daily_df = daily_df.set_index("DATETIME")

            # Ratio strength
            merged = daily_df.join(
                nifty_df["CLOSE"],
                how="inner",
                rsuffix="_NIFTY"
            )

            merged["RATIO"] = merged["CLOSE"] / merged["CLOSE_NIFTY"]
            merged["RATIO_EMA"] = merged["RATIO"].ewm(span=20).mean()

            ratio_strong = (
                merged["RATIO"].iloc[-1] >
                merged["RATIO_EMA"].iloc[-1]
            )

            # Run TD Engine
            engine_daily = DeMarkEngine(daily_df.reset_index())
            daily = engine_daily.run()
            last_daily = daily.iloc[-1]

            # ================= 1H ================= #

            hourly_df = fetch_data(token, "60", 30)
            if hourly_df is None or len(hourly_df) < 50:
                continue

            engine_hour = DeMarkEngine(hourly_df)
            hourly = engine_hour.run()
            last_hour = hourly.iloc[-1]

            # ================= CLASSIFICATION ================= #

            signal = None
            confidence = 0

            bullish_bias = last_daily["close"] > daily_df["EMA200"].iloc[-1]

            # Fresh Buy
            if bullish_bias and last_hour["valid_buy_13"]:
                signal = "Fresh Buy"
                confidence += 2

            # Strong Sell
            if last_daily["valid_sell_13"] and last_hour["valid_sell_13"]:
                signal = "Strong Sell"
                confidence += 2

            # Early Exhaustion
            if last_daily["td9_buy"]:
                signal = "Early Buy Exhaustion"

            if last_daily["td9_sell"]:
                signal = "Early Sell Exhaustion"

            # Intraday Exhaustion
            if last_hour["td9_buy"]:
                signal = "Intraday Buy Exhaustion"

            if last_hour["td9_sell"]:
                signal = "Intraday Sell Exhaustion"

            # ================= CONFIDENCE ================= #

            if last_daily["valid_buy_13"]:
                confidence += 2

            if last_hour["valid_buy_13"]:
                confidence += 1

            if ratio_strong:
                confidence += 1

            if last_hour["buy_age"] <= 3:
                confidence += 1

            # ================= FINAL FILTER ================= #

            if confidence >= 4 and signal:

                leadership = "Leader" if ratio_strong else "Lagging"

                results.append(
                    f"{symbol:<12} | {signal:<25} | Score {confidence}/6 | {leadership}"
                )

        except:
            continue

    logging.info(f"Scanned: {total_scanned}")
    logging.info(f"Signals: {len(results)}")

    if not results:
        logging.info("No high-confidence signals.")
        return

    message = (
        "📊 TD Framework v1.1 Signals\n"
        f"Scanned: {total_scanned}\n\n"
        + "\n".join(results[:40])
    )

    send_telegram(message)


if __name__ == "__main__":
    run()
