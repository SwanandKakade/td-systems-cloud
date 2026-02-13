import os
import requests
import pandas as pd
import zipfile
import io
import logging
from datetime import datetime, timedelta
from demark_engine import DeMarkEngine



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


def fetch_240m(token):
    try:
        end = datetime.now()
        start = end - timedelta(days=60)   # 240m needs less history

        url = f"https://data.definedgesecurities.com/sds/history/NSE/{token}/240/{start.strftime('%d%m%Y')}0000/{end.strftime('%d%m%Y')}2359"

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

    logging.info("Starting TD Institutional Scanner...")

    master = load_master_file()
    if master is None:
        logging.error("Master unavailable. Exiting.")
        return

    signals = []
    total_scanned = 0

    for _, row in tqdm(master.iterrows(), total=len(master)):

        token = row["TOKEN"]
        symbol = row["SYMBOL"]

        total_scanned += 1

        try:
            # ==========================
            # 1️⃣ DAILY DATA
            # ==========================
            df_daily = fetch_daily(token)

            if df_daily is None or len(df_daily) < 50:
                continue

            if df_daily["VOLUME"].tail(20).mean() < 100000:
                continue

            engine_daily = DeMarkEngine(df_daily)
            daily = engine_daily.run()
            last_daily = daily.iloc[-1]

            # ==========================
            # 2️⃣ OPTIONAL 240m DATA
            # ==========================
            last_240 = None

            if (
                last_daily["valid_buy_13"] or
                last_daily["valid_sell_13"] or
                last_daily["bull_countdown"] >= 10 or
                last_daily["bear_countdown"] >= 10
            ):

                df_240 = fetch_240m(token)

                if df_240 is not None and len(df_240) >= 50:
                    engine_240 = DeMarkEngine(df_240)
                    h240 = engine_240.run()
                    last_240 = h240.iloc[-1]

            # ==========================
            # 3️⃣ CONFIDENCE SCORING
            # ==========================
            buy_conf = 0
            sell_conf = 0

            # Daily weight
            if last_daily["valid_buy_13"]:
                buy_conf += 2
            if last_daily["perfect_buy"]:
                buy_conf += 1

            if last_daily["valid_sell_13"]:
                sell_conf += 2
            if last_daily["perfect_sell"]:
                sell_conf += 1

            # 240m weight
            if last_240 is not None:

                if last_240["valid_buy_13"]:
                    buy_conf += 2
                if last_240["perfect_buy"]:
                    buy_conf += 1

                if last_240["valid_sell_13"]:
                    sell_conf += 2
                if last_240["perfect_sell"]:
                    sell_conf += 1

            # ==========================
            # 4️⃣ BUILD SIGNALS
            # ==========================

            # Strong Buy
            if buy_conf >= 4:
                signals.append(
                    f"💎 {symbol} STRONG BUY | Confidence: {buy_conf}"
                )

            # Strong Sell
            if sell_conf >= 4:
                signals.append(
                    f"⚡ {symbol} STRONG SELL | Confidence: {sell_conf}"
                )

            # Early setups (density)
            if last_daily["bull_setup"] in [7, 8]:
                signals.append(
                    f"🟢 {symbol} Daily Bull Setup {int(last_daily['bull_setup'])}"
                )

            if last_daily["bear_setup"] in [7, 8]:
                signals.append(
                    f"🔴 {symbol} Daily Bear Setup {int(last_daily['bear_setup'])}"
                )

            # Countdown progress
            if last_daily["bull_countdown"] in [10, 11, 12]:
                signals.append(
                    f"🟢 {symbol} Daily Bull Countdown {int(last_daily['bull_countdown'])}"
                )

            if last_daily["bear_countdown"] in [10, 11, 12]:
                signals.append(
                    f"🔴 {symbol} Daily Bear Countdown {int(last_daily['bear_countdown'])}"
                )

            # Final 13 Exhaustion
            if last_daily["valid_buy_13"]:
                signals.append(
                    f"🚀 {symbol} DAILY 13 BUY Exhaustion"
                )

            if last_daily["valid_sell_13"]:
                signals.append(
                    f"🔥 {symbol} DAILY 13 SELL Exhaustion"
                )

        except Exception as e:
            logging.warning(f"Error processing {symbol}: {e}")
            continue

    # ==========================
    # 5️⃣ TELEGRAM SECTION
    # ==========================

    logging.info(f"Scan Completed. Symbols scanned: {total_scanned}")
    logging.info(f"Signals Found: {len(signals)}")

    if not signals:
        logging.info("No institutional signals detected.")
        return

    # Remove duplicates
    signals = list(dict.fromkeys(signals))

    message = (
        "📊 TD Institutional Signals\n"
        f"Scanned: {total_scanned}\n\n"
        + "\n".join(signals[:40])
    )

    logging.info("Sending Telegram alert...")
    send_telegram(message)

if __name__ == "__main__":
    run()
