import os
import requests
import pandas as pd
import zipfile
import io
import logging
from datetime import datetime, timedelta
from demark_engine import DeMarkEngine

# Safe tqdm (Railway safe)
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


# ---------------- HISTORY ---------------- #

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


# ---------------- RUN ---------------- #

def run():

    logging.info("Starting TD Institutional Scanner...")

    master = load_master_file()
    if master is None:
        logging.error("Master unavailable.")
        return

    rows = []
    total_scanned = 0

    for _, row in tqdm(master.iterrows(), total=len(master)):

        token = row["TOKEN"]
        symbol = row["SYMBOL"]
        total_scanned += 1

        try:

            # ---------------- DAILY ---------------- #

            df_daily = fetch_daily(token)
            if df_daily is None or len(df_daily) < 100:
                continue

            if df_daily["VOLUME"].tail(20).mean() < 100000:
                continue

            # 200 EMA Filter
            df_daily["EMA200"] = df_daily["CLOSE"].ewm(span=200).mean()
            trend_up = df_daily.iloc[-1]["CLOSE"] > df_daily.iloc[-1]["EMA200"]

            engine_daily = DeMarkEngine(df_daily)
            daily = engine_daily.run()

            recent_daily = daily.tail(12)

            # ---------------- 1H ---------------- #

            df_1h = fetch_1h(token)
            if df_1h is None or len(df_1h) < 100:
                continue

            engine_1h = DeMarkEngine(df_1h)
            h1 = engine_1h.run()

            recent_1h = h1.tail(12)

            # ---------------- SIGNAL DETECTION ---------------- #

            def detect_signal(df_recent):

                buy13 = df_recent[df_recent["td13_buy_status"].notna()]
                sell13 = df_recent[df_recent["td13_sell_status"].notna()]
                buy9 = df_recent[df_recent["td9_buy_status"].notna()]
                sell9 = df_recent[df_recent["td9_sell_status"].notna()]

                if not buy13.empty:
                    return buy13.iloc[-1]["td13_buy_status"] + " Buy", 3

                if not sell13.empty:
                    return sell13.iloc[-1]["td13_sell_status"] + " Sell", 3

                if not buy9.empty:
                    return buy9.iloc[-1]["td9_buy_status"], 1

                if not sell9.empty:
                    return sell9.iloc[-1]["td9_sell_status"], 1

                return None, 0


            daily_signal, daily_score = detect_signal(recent_daily)
            h1_signal, h1_score = detect_signal(recent_1h)

            if not daily_signal and not h1_signal:
                continue

            # ---------------- CONFIDENCE ---------------- #

            confidence = daily_score + h1_score

            if trend_up:
                confidence += 1

            # ---------------- FINAL SIGNAL LABEL ---------------- #

            final_signal = daily_signal if daily_signal else h1_signal

            rows.append({
                "Stock": symbol,
                "1H": h1_signal if h1_signal else "-",
                "Daily": daily_signal if daily_signal else "-",
                "Signal": final_signal,
                "Conf": confidence
            })

        except Exception as e:
            logging.warning(f"Error processing {symbol}: {e}")
            continue

    # ---------------- TELEGRAM OUTPUT ---------------- #

    logging.info(f"Scanned: {total_scanned}")
    logging.info(f"Signals Found: {len(rows)}")

    if not rows:
        logging.info("No signals detected.")
        return

    rows = sorted(rows, key=lambda x: x["Conf"], reverse=True)

    message = "📊 TD Institutional Signals\n"
    message += f"Scanned: {total_scanned}\n\n"
    message += "Stock | 1H | Daily | Signal | Conf\n"
    message += "-" * 45 + "\n"

    for r in rows[:25]:
        message += f"{r['Stock']} | {r['1H']} | {r['Daily']} | {r['Signal']} | {r['Conf']}\n"

    send_telegram(message)


if __name__ == "__main__":
    run()
