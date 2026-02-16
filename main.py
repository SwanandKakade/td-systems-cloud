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
    
def fetch_yesterday_close(token):
    try:
        end = datetime.now()
        start = end - timedelta(days=3)  # small buffer for weekends

        from_str = start.strftime("%d%m%Y") + "0000"
        to_str   = end.strftime("%d%m%Y") + "2359"

        url = (
            f"https://data.definedgesecurities.com/sds/history/NSE/"
            f"{token}/day/{from_str}/{to_str}"
        )
        logging.warning(url)
        headers = {"Authorization": DEFINEDGE_SESSION.strip()}

        r = requests.get(url, headers=headers, timeout=10)
        logging.warning(r)
        print(r.text[:500])
        if r.status_code != 200:
            logging.warning(f"NIFTY HTTP error: {r.status_code}")
            return None

        if not r.text.strip():
            return None

        # Parse manually instead of pandas mixed parsing
        rows = []
        for line in r.text.strip().split("\n"):
            parts = line.split(",")
            if len(parts) >= 6:
                rows.append(parts)

        if not rows:
            return None

        # Last valid row = latest trading day
        last_row = rows[-1]

        close_price = float(last_row[4])  # CLOSE column
        print(close_price)
        return close_price

    except Exception as e:
        logging.warning(f"NIFTY fetch error: {e}")
        return None


def load_master_file():
    try:
        response = requests.get(MASTER_URL, timeout=10)
        response.raise_for_status()

        z = zipfile.ZipFile(io.BytesIO(response.content))
        df = pd.read_csv(z.open(z.namelist()[0]))

        df.columns = [
            "EXCHANGE","TOKEN","SYMBOL","TRADINGSYM",
            "INSTRUMENTTYPE","EXPIRY","TICKSIZE","LOTSIZE",
            "OPTIONTYPE","STRIKE","PRICEPREC","MULTIPLIER",
            "ISIN","PRICEMULT","COMPANY"
        ]

        # ✅ Keep only tradable instruments
        df = df[df["INSTRUMENTTYPE"].isin(["EQ", "IDX"])]

        # ✅ Remove weird trading series like BE / BL if present
        df = df[~df["TRADINGSYM"].str.contains("-BE|-BL", na=False)]

        # ✅ Ensure token is numeric
        df["TOKEN"] = pd.to_numeric(df["TOKEN"], errors="coerce")
        df = df.dropna(subset=["TOKEN"])
        df = df.reset_index(drop=True)

        logging.info(f"Master filtered symbols: {len(df)}")

        return df

    except Exception as e:
        logging.error(f"Master load failed: {e}")
        return None

def fetch_data(token, timeframe, days):

    try:
        end = datetime.now()
        start = end - timedelta(days=days)

        # --------------------------
        # Build date strings
        # --------------------------

        if timeframe == "day":
            from_str = start.strftime("%d%m%Y") + "0000"
            to_str   = end.strftime("%d%m%Y") + "2359"

        elif timeframe == "minute":
            from_str = start.strftime("%d%m%Y%H%M")
            to_str   = end.strftime("%d%m%Y%H%M")

        else:
            logging.warning(f"Invalid timeframe requested: {timeframe}")
            return None

        url = (
            f"https://data.definedgesecurities.com/sds/history/NSE/"
            f"{token}/{timeframe}/{from_str}/{to_str}"
        )

        headers = {"Authorization": DEFINEDGE_SESSION.strip()}

        r = requests.get(url, headers=headers, timeout=10)

        if r.status_code != 200:
            logging.warning(f"HTTP error {token}: {r.status_code}")
            return None

        if not r.text.strip():
            return None

        # --------------------------
        # Read CSV safely
        # --------------------------

        df = pd.read_csv(
            io.StringIO(r.text),
            header=None
        )

        if df.empty:
            return None

        df.columns = ["DATETIME","OPEN","HIGH","LOW","CLOSE","VOLUME"]

        # --------------------------
        # Correct datetime parsing
        # Definedge format: ddmmyyyyHHMM
        # --------------------------

        df["DATETIME"] = pd.to_datetime(
            df["DATETIME"].astype(str),
            format="%d%m%Y%H%M",
            errors="coerce"
        )

        df = df.dropna(subset=["DATETIME"])

        if df.empty:
            return None

        df = df.sort_values("DATETIME")

        # --------------------------
        # If minute timeframe, resample to 60min
        # --------------------------

        if timeframe == "minute":

            df = df.set_index("DATETIME")

            df = df.resample("60min").agg({
                "OPEN": "first",
                "HIGH": "max",
                "LOW": "min",
                "CLOSE": "last",
                "VOLUME": "sum"
            })

            df = df.dropna()
            df = df.reset_index()

        return df

    except Exception as e:
        logging.warning(f"Fetch error {token}: {e}")
        return None

def run():

    logging.info("Starting TD Framework v3.0")

    results = []

    master = load_master_file()

    # Fetch yesterday NIFTY close once
    nifty_close = fetch_yesterday_close(26000)

    if nifty_close is None:
        logging.warning("Failed to fetch NIFTY close")
        return

    logging.info(f"NIFTY Close: {nifty_close}")

    for _, row in tqdm(master.iterrows(), total=len(master)):

        try:
            token = row["TOKEN"]
            symbol = row["SYMBOL"]

            daily_df = fetch_data(token, "day", 200)
            hourly_df = fetch_data(token, "minute", 10)

            if daily_df is None or hourly_df is None:
                continue

            if daily_df.empty or hourly_df.empty:
                continue

            # =========================
            # Bias (EMA200)
            # =========================

            daily_df["EMA200"] = daily_df["CLOSE"].ewm(span=200, adjust=False).mean()

            bullish_bias = daily_df["CLOSE"].iloc[-1] > daily_df["EMA200"].iloc[-1]
            bias = "Bullish" if bullish_bias else "Bearish"

            # =========================
            # Run DeMark Engine
            # =========================

            daily_engine = DeMarkEngine(daily_df)
            hour_engine = DeMarkEngine(hourly_df)

            daily = daily_engine.run()
            hourly = hour_engine.run()

            last_daily = daily.iloc[-1]
            last_hour = hourly.iloc[-1]

            # =========================
            # Ratio Leadership (Using Only NIFTY Close)
            # =========================

            ratio_series = daily_df["CLOSE"] / nifty_close
            ratio_ema = ratio_series.ewm(span=30, adjust=False).mean()

            ratio_strong = ratio_series.iloc[-1] > ratio_ema.iloc[-1]
            leadership = "Leader" if ratio_strong else "Lagging"

            # =========================
            # Clean Classification
            # =========================

            classification = "Neutral"

            if last_daily["TD13_SELL_STATUS"] in ["Fresh", "Active"] and \
               last_hour["TD13_SELL_STATUS"] in ["Fresh", "Active"]:
                classification = "Strong Sell"

            elif bullish_bias and \
                 last_hour["TD13_BUY_STATUS"] in ["Fresh", "Active"]:
                classification = "Fresh Buy"

            elif last_daily["TD9_BUY_STATUS"] in ["Fresh", "Active"]:
                classification = "Early Buy Exhaustion"

            elif last_daily["TD9_SELL_STATUS"] in ["Fresh", "Active"]:
                classification = "Early Sell Exhaustion"

            elif last_hour["TD9_BUY_STATUS"] in ["Fresh", "Active"]:
                classification = "Intraday Buy Exhaustion"

            elif last_hour["TD9_SELL_STATUS"] in ["Fresh", "Active"]:
                classification = "Intraday Sell Exhaustion"

            # =========================
            # Confidence Scoring (0–6)
            # =========================

            confidence = 0

            if last_daily["TD13_BUY_STATUS"] in ["Fresh", "Active"] or \
               last_daily["TD13_SELL_STATUS"] in ["Fresh", "Active"]:
                confidence += 2

            if last_hour["TD13_BUY_STATUS"] in ["Fresh", "Active"] or \
               last_hour["TD13_SELL_STATUS"] in ["Fresh", "Active"]:
                confidence += 1

            if ratio_strong:
                confidence += 1

            if last_hour["TD13_BUY_AGE"] <= 3 or \
               last_hour["TD13_SELL_AGE"] <= 3:
                confidence += 1

            if bullish_bias and \
               last_hour["TD13_BUY_STATUS"] in ["Fresh", "Active"]:
                confidence += 1

            # =========================
            # Append Result
            # =========================

            results.append(
                f"{symbol:<12} | {classification:<22} | "
                f"Bias: {bias:<8} | {leadership:<8} | "
                f"Score: {confidence}/6"
            )

        except Exception as e:
            logging.warning(f"Error processing {symbol}: {e}")
            continue

    # =========================
    # Send Telegram
    # =========================

    if not results:
        logging.info("No signals generated.")
        return

    message = "📊 Signals\n\n" + "\n".join(results[:500])

    send_telegram(message)

    logging.info("Telegram message sent successfully.")

if __name__ == "__main__":
    run()
