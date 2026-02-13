import requests
import pandas as pd
import io
import zipfile
import time
from datetime import datetime, timedelta
from pandas.errors import EmptyDataError

# ==============================
# CONFIG
# ==============================

SESSION_KEY = "YOUR_SESSION_KEY"
MASTER_URL = "https://app.definedgesecurities.com/public/nsecash.zip"
BASE_HISTORY_URL = "https://data.definedgesecurities.com/sds/history"

HEADERS = {"Authorization": SESSION_KEY}

DAILY_LOOKBACK_DAYS = 400
MIN_VOLUME = 200000  # Volume filter
SLEEP_SECONDS = 0.15  # API rate protection


# ==============================
# MASTER FILE LOADER
# ==============================

def load_master_file():
    print("Downloading NSE Cash master file...")

    try:
        response = requests.get(MASTER_URL, timeout=20)

        if response.status_code != 200:
            print("Master download failed:", response.status_code)
            return None

        if not response.content.startswith(b'PK'):
            print("Invalid master response (not ZIP).")
            return None

        z = zipfile.ZipFile(io.BytesIO(response.content))
        file_name = z.namelist()[0]
        df = pd.read_csv(z.open(file_name))

        print("Master file loaded. Rows:", len(df))
        return df

    except Exception as e:
        print("Master file error:", e)
        return None


# ==============================
# FETCH HISTORY (SAFE)
# ==============================

def fetch_history(segment, token, timeframe="day"):
    try:
        to_date = datetime.now().strftime("%d%m%Y1530")
        from_date = (datetime.now() - timedelta(days=DAILY_LOOKBACK_DAYS)).strftime("%d%m%Y0915")

        url = f"{BASE_HISTORY_URL}/{segment}/{token}/{timeframe}/{from_date}/{to_date}"

        response = requests.get(url, headers=HEADERS, timeout=20)

        if response.status_code != 200:
            return None

        if not response.text.strip():
            return None

        df = pd.read_csv(io.StringIO(response.text), header=None)

        if df.empty:
            return None

        # Assign columns
        df.columns = ["DATETIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]

        return df

    except EmptyDataError:
        return None
    except Exception:
        return None


# ==============================
# TD SEQUENTIAL (Basic Setup)
# ==============================

def td_setup(df):
    df = df.copy()
    df["CLOSE"] = pd.to_numeric(df["CLOSE"], errors="coerce")
    df = df.dropna()

    df["TD"] = 0

    for i in range(4, len(df)):
        if df["CLOSE"].iloc[i] > df["CLOSE"].iloc[i - 4]:
            df.loc[df.index[i], "TD"] = df["TD"].iloc[i - 1] + 1
        else:
            df.loc[df.index[i], "TD"] = 0

    return df


# ==============================
# MAIN RUN LOGIC
# ==============================

def run():
    print("Starting TD Sequential Scanner...")

    master_df = load_master_file()

    if master_df is None:
        print("Master unavailable. Exiting safely.")
        return

    if "TOKEN" not in master_df.columns:
        print("TOKEN column missing in master.")
        return

    universe = master_df.dropna(subset=["TOKEN"])
    print("Filtered NSE Cash universe:", len(universe))

    shortlisted = []

    total = len(universe)
    count = 0

    for _, row in universe.iterrows():
        count += 1
        symbol = str(row.get("SYMBOL", "UNKNOWN"))
        token = row["TOKEN"]

        print(f"[{count}/{total}] Scanning: {symbol}")

        df_daily = fetch_history("NSE", token, "day")
        time.sleep(SLEEP_SECONDS)

        if df_daily is None:
            continue

        # Volume filter
        df_daily["VOLUME"] = pd.to_numeric(df_daily["VOLUME"], errors="coerce")
        if df_daily["VOLUME"].iloc[-1] < MIN_VOLUME:
            continue

        df_daily = td_setup(df_daily)

        latest_td = df_daily["TD"].iloc[-1]

        # Near 7/8/9 filter
        if latest_td in [7, 8, 9]:
            print("  → Near TD Setup:", latest_td)
            shortlisted.append((symbol, token, latest_td))

    print("\nShortlisted Stocks:", len(shortlisted))

    # ==============================
    # FETCH MINUTE DATA ONLY FOR SHORTLIST
    # ==============================

    for symbol, token, td_val in shortlisted:
        print(f"Fetching minute data for {symbol} (TD={td_val})")

        df_min = fetch_history("NSE", token, "minute")
        time.sleep(SLEEP_SECONDS)

        if df_min is None:
            print("  → Minute data unavailable")
            continue

        print(f"  → Minute candles fetched: {len(df_min)}")

    print("\nScan Complete.")


# ==============================
# SAFE ENTRY POINT
# ==============================

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("Fatal error caught safely:", e)
