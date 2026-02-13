import requests
import pandas as pd
import zipfile
import io
import os
from datetime import datetime, timedelta
from io import StringIO

# ===============================
# CONFIG
# ===============================
SESSION_KEY = os.getenv("DEFINEDGE_SESSION_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

BASE_URL = "https://data.definedgesecurities.com"
MASTER_FNO_URL = "https://app.definedgesecurities.com/public/nsefno.zip"

# ===============================
# TELEGRAM
# ===============================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    requests.post(url, data=payload)

# ===============================
# FETCH F&O UNIVERSE
# ===============================
def get_fno_universe():
    response = requests.get(MASTER_FNO_URL)
    z = zipfile.ZipFile(io.BytesIO(response.content))
    filename = z.namelist()[0]
    df = pd.read_csv(z.open(filename))

    print("MASTER COLUMNS:", df.columns.tolist())
    return df.head(5)


def fetch_daily(token):
    end = datetime.today()
    start = end - timedelta(days=200)

    url = f"{BASE_URL}/sds/history/NSE/{int(token)}/day?from={start.strftime('%Y-%m-%d')}&to={end.strftime('%Y-%m-%d')}"

    headers = {
        "api_session_key": SESSION_KEY
    }

    response = requests.get(url, headers=headers, timeout=15)

    if response.status_code != 200:
        print("History fetch failed:", response.status_code, response.text)
        return None

    df = pd.read_csv(StringIO(response.text))
    return df



# ===============================
# TD CALCULATION
# ===============================
def calculate_td(df):
    df = df.copy()

    # TD9 Setup
    df["buy_setup"] = df["Close"] < df["Close"].shift(4)
    df["sell_setup"] = df["Close"] > df["Close"].shift(4)

    df["buy_count"] = df["buy_setup"].groupby((df["buy_setup"] == False).cumsum()).cumcount() + 1
    df["buy_count"] = df["buy_count"] * df["buy_setup"]

    df["sell_count"] = df["sell_setup"].groupby((df["sell_setup"] == False).cumsum()).cumcount() + 1
    df["sell_count"] = df["sell_count"] * df["sell_setup"]

    # TD13 Countdown
    df["buy_cd"] = df["Close"] <= df["Low"].shift(2)
    df["sell_cd"] = df["Close"] >= df["High"].shift(2)

    df["buy_cd_count"] = df["buy_cd"].groupby((df["buy_cd"] == False).cumsum()).cumcount() + 1
    df["buy_cd_count"] = df["buy_cd_count"] * df["buy_cd"]

    df["sell_cd_count"] = df["sell_cd"].groupby((df["sell_cd"] == False).cumsum()).cumcount() + 1
    df["sell_cd_count"] = df["sell_cd_count"] * df["sell_cd"]

    latest = df.iloc[-1]

    result = {
        "new_today": None,
        "active": None,
        "early": None,
        "score": 0
    }

    # NEW TODAY
    if latest["buy_count"] == 9:
        result["new_today"] = "TD9 BUY"
        result["score"] += 2

    if latest["sell_count"] == 9:
        result["new_today"] = "TD9 SELL"
        result["score"] += 2

    if latest["buy_cd_count"] == 13:
        result["new_today"] = "TD13 BUY"
        result["score"] += 4

    if latest["sell_cd_count"] == 13:
        result["new_today"] = "TD13 SELL"
        result["score"] += 4

    # ACTIVE
    if 9 < latest["buy_count"] <= 12:
        result["active"] = "TD9 BUY"
        result["score"] += 1

    if 9 < latest["sell_count"] <= 12:
        result["active"] = "TD9 SELL"
        result["score"] += 1

    if 13 < latest["buy_cd_count"] <= 20:
        result["active"] = "TD13 BUY"
        result["score"] += 2

    if 13 < latest["sell_cd_count"] <= 20:
        result["active"] = "TD13 SELL"
        result["score"] += 2

    # EARLY
    if latest["buy_count"] in [7, 8]:
        result["early"] = f"TD9 BUY ({int(latest['buy_count'])})"

    if latest["sell_count"] in [7, 8]:
        result["early"] = f"TD9 SELL ({int(latest['sell_count'])})"

    if latest["buy_cd_count"] in [11, 12]:
        result["early"] = f"TD13 BUY ({int(latest['buy_cd_count'])})"

    if latest["sell_cd_count"] in [11, 12]:
        result["early"] = f"TD13 SELL ({int(latest['sell_cd_count'])})"

    return result

# ===============================
# MAIN ENGINE
# ===============================
def run():
    universe = get_fno_universe()

    new_today = []
    active = []
    early = []

    for _, row in universe.iterrows():
        symbol = row["SYMBOL"]
        token = row["TOKEN"]

        df = fetch_daily(token)
        if df is None or len(df) < 20:
            continue

        result = calculate_td(df)

        if result["new_today"]:
            new_today.append((symbol, result["new_today"], result["score"]))

        elif result["active"]:
            active.append((symbol, result["active"], result["score"]))

        elif result["early"]:
            early.append((symbol, result["early"], result["score"]))

    new_today.sort(key=lambda x: x[2], reverse=True)
    active.sort(key=lambda x: x[2], reverse=True)

    message = f"TD SYSTEMS – DAILY SCAN ({datetime.today().strftime('%d %b %Y')})\n\n"

    message += "=== NEW TODAY ===\n"
    for s in new_today[:10]:
        message += f"{s[0]} → {s[1]} (Score {s[2]})\n"

    message += "\n=== ACTIVE ===\n"
    for s in active[:10]:
        message += f"{s[0]} → {s[1]} (Score {s[2]})\n"

    message += "\n=== EARLY ===\n"
    for s in early[:10]:
        message += f"{s[0]} → {s[1]}\n"

    send_telegram(message)

if __name__ == "__main__":
    run()
