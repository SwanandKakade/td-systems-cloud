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


DEFINEDGE_SESSION = os.getenv("DEFINEDGE_SESSION_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

MASTER_URL = "https://app.definedgesecurities.com/public/nsecash.zip"
NIFTY_TOKEN = "26000"

logging.basicConfig(level=logging.INFO, format="%(message)s")


def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": message})


def load_master_file():
    response = requests.get(MASTER_URL)
    z = zipfile.ZipFile(io.BytesIO(response.content))
    df = pd.read_csv(z.open(z.namelist()[0]))
    df.columns = [
        "EXCHANGE","TOKEN","SYMBOL","TRADINGSYM",
        "INSTRUMENTTYPE","EXPIRY","TICKSIZE","LOTSIZE",
        "OPTIONTYPE","STRIKE","PRICEPREC","MULTIPLIER",
        "ISIN","PRICEMULT","COMPANY"
    ]
    return df[df["INSTRUMENTTYPE"] == "EQ"]


def fetch_data(token, timeframe, days):

    end = datetime.now()
    start = end - timedelta(days=days)

    url = (
        f"https://data.definedgesecurities.com/sds/history/NSE/"
        f"{token}/{timeframe}/"
        f"{start.strftime('%d%m%Y')}0000/"
        f"{end.strftime('%d%m%Y')}2359"
    )

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


def run():

    logging.info("Starting TD Framework v3.0")

    master = load_master_file()
    nifty = fetch_data(NIFTY_TOKEN, "day", 200)
    nifty = nifty.set_index("DATETIME")

    results = []

    for _, row in tqdm(master.iterrows(), total=len(master)):

        token = row["TOKEN"]
        symbol = row["SYMBOL"]

        daily_df = fetch_data(token, "day", 200)
        hourly_df = fetch_data(token, "60", 30)

        if daily_df is None or hourly_df is None:
            continue

        daily_df["EMA200"] = daily_df["CLOSE"].ewm(span=200).mean()

        daily_engine = DeMarkEngine(daily_df)
        hour_engine = DeMarkEngine(hourly_df)

        daily = daily_engine.run()
        hourly = hour_engine.run()

        last_daily = daily.iloc[-1]
        last_hour = hourly.iloc[-1]

        bullish_bias = daily_df["CLOSE"].iloc[-1] > daily_df["EMA200"].iloc[-1]
        bias = "Bullish" if bullish_bias else "Bearish"

        # Ratio
        merged = daily_df.set_index("DATETIME").join(
            nifty["CLOSE"],
            how="inner",
            rsuffix="_NIFTY"
        )

        merged["RATIO"] = merged["CLOSE"] / merged["CLOSE_NIFTY"]
        merged["RATIO_EMA"] = merged["RATIO"].ewm(span=20).mean()

        ratio_strong = merged["RATIO"].iloc[-1] > merged["RATIO_EMA"].iloc[-1]
        leadership = "Leader" if ratio_strong else "Lagging"

        # Clean Classification

        classification = "Neutral"

        if last_daily["TD13_SELL_STATUS"] in ["Fresh","Active"] and \
           last_hour["TD13_SELL_STATUS"] in ["Fresh","Active"]:
            classification = "Strong Sell"

        elif bullish_bias and \
             last_hour["TD13_BUY_STATUS"] in ["Fresh","Active"]:
            classification = "Fresh Buy"

        elif last_daily["TD9_BUY_STATUS"] in ["Fresh","Active"]:
            classification = "Early Buy Exhaustion"

        elif last_daily["TD9_SELL_STATUS"] in ["Fresh","Active"]:
            classification = "Early Sell Exhaustion"

        elif last_hour["TD9_BUY_STATUS"] in ["Fresh","Active"]:
            classification = "Intraday Buy Exhaustion"

        elif last_hour["TD9_SELL_STATUS"] in ["Fresh","Active"]:
            classification = "Intraday Sell Exhaustion"

        # Confidence

        confidence = 0

        if last_daily["TD13_BUY_STATUS"] in ["Fresh","Active"] or \
           last_daily["TD13_SELL_STATUS"] in ["Fresh","Active"]:
            confidence += 2

        if last_hour["TD13_BUY_STATUS"] in ["Fresh","Active"] or \
           last_hour["TD13_SELL_STATUS"] in ["Fresh","Active"]:
            confidence += 1

        if ratio_strong:
            confidence += 1

        if last_hour["TD13_BUY_AGE"] <= 3 or \
           last_hour["TD13_SELL_AGE"] <= 3:
            confidence += 1

        if bullish_bias and \
           last_hour["TD13_BUY_STATUS"] in ["Fresh","Active"]:
            confidence += 1

        results.append(
            f"{symbol:<12} | {classification:<22} | "
            f"Bias: {bias:<8} | {leadership:<8} | "
            f"Score: {confidence}/6"
        )

    message = "📊 Signal\n\n" + "\n".join(results[:40])
    send_telegram(message)


if __name__ == "__main__":
    run()
