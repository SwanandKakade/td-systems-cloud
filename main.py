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

# ⚠️ Replace with correct NIFTY token from master file
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


# ================= DATA FETCH ================= #

def fetch_data(token, timeframe, days):
    try:
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

    except:
        return None


# ================= RUN ================= #

def run():

    logging.info("Starting TD Framework v2.0 Dashboard Scanner...")

    master = load_master_file()
    if master is None:
        logging.error("Master unavailable.")
        return

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

            engine_daily = DeMarkEngine(daily_df.reset_index())
            daily = engine_daily.run()
            last_daily = daily.iloc[-1]

            # Bias
            bullish_bias = daily_df["CLOSE"].iloc[-1] > daily_df["EMA200"].iloc[-1]
            bias = "Bullish" if bullish_bias else "Bearish"

            # ================= 1H ================= #

            hourly_df = fetch_data(token, "60", 30)
            if hourly_df is None or len(hourly_df) < 50:
                continue

            engine_hour = DeMarkEngine(hourly_df)
            hourly = engine_hour.run()
            last_hour = hourly.iloc[-1]

            # ================= RATIO ================= #

            merged = daily_df.join(
                nifty_df["CLOSE"],
                how="inner",
                rsuffix="_NIFTY"
            )

            merged["RATIO"] = merged["CLOSE"] / merged["CLOSE_NIFTY"]
            merged["RATIO_EMA"] = merged["RATIO"].ewm(span=20).mean()

            ratio_strong = merged["RATIO"].iloc[-1] > merged["RATIO_EMA"].iloc[-1]
            leadership = "Leader" if ratio_strong else "Lagging"

            # ================= SIGNAL STATES ================= #

            daily_state = "None"
            if last_daily["valid_buy_13"]:
                daily_state = f"TD13 Buy ({last_daily['buy_status']})"
            elif last_daily["valid_sell_13"]:
                daily_state = f"TD13 Sell ({last_daily['sell_status']})"
            elif last_daily["td9_buy"]:
                daily_state = "TD9 Buy"
            elif last_daily["td9_sell"]:
                daily_state = "TD9 Sell"

            hour_state = "None"
            if last_hour["valid_buy_13"]:
                hour_state = f"TD13 Buy ({last_hour['buy_status']})"
            elif last_hour["valid_sell_13"]:
                hour_state = f"TD13 Sell ({last_hour['sell_status']})"
            elif last_hour["td9_buy"]:
                hour_state = "TD9 Buy"
            elif last_hour["td9_sell"]:
                hour_state = "TD9 Sell"

            # ================= CONFIDENCE ================= #

            confidence = 0

            if last_daily["valid_buy_13"] or last_daily["valid_sell_13"]:
                confidence += 2

            if last_hour["valid_buy_13"] or last_hour["valid_sell_13"]:
                confidence += 1

            if ratio_strong:
                confidence += 1

            if (
                last_hour["buy_age"] <= 3 or
                last_hour["sell_age"] <= 3
            ):
                confidence += 1

            if bullish_bias and last_hour["valid_buy_13"]:
                confidence += 1

            # ================= FINAL CLASSIFICATION ================= #

            classification = "Neutral"

            if last_daily["valid_sell_13"] and last_hour["valid_sell_13"]:
                classification = "Strong Sell"

            elif bullish_bias and last_hour["valid_buy_13"]:
                classification = "Fresh Buy"

            elif last_daily["td9_buy"]:
                classification = "Early Buy Exhaustion"

            elif last_daily["td9_sell"]:
                classification = "Early Sell Exhaustion"

            elif last_hour["td9_buy"]:
                classification = "Intraday Buy Exhaustion"

            elif last_hour["td9_sell"]:
                classification = "Intraday Sell Exhaustion"

            # ================= APPEND ================= #

            results.append({
                "symbol": symbol,
                "bias": bias,
                "daily": daily_state,
                "hour": hour_state,
                "leadership": leadership,
                "confidence": confidence,
                "classification": classification
            })

        except:
            continue

    # ================= OUTPUT ================= #

    logging.info(f"Scanned: {total_scanned}")
    logging.info(f"Total Instruments: {len(results)}")

    if not results:
        logging.info("No data.")
        return

    # Sort by confidence
    results = sorted(results, key=lambda x: x["confidence"], reverse=True)

    lines = []

    for r in results[:40]:
        lines.append(
            f"{r['symbol']:<12} | "
            f"{r['classification']:<22} | "
            f"Bias: {r['bias']:<7} | "
            f"D: {r['daily']:<18} | "
            f"H: {r['hour']:<18} | "
            f"{r['leadership']:<8} | "
            f"{r['confidence']}/6"
        )

    message = (
        "📊 Dashboard\n"
        f"Scanned: {total_scanned}\n\n"
        + "\n".join(lines)
    )

    send_telegram(message)


if __name__ == "__main__":
    run()
