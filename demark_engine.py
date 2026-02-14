import pandas as pd
import numpy as np


class DeMarkEngine:

    def __init__(self, df):
        self.df = df.copy()
        self.df.columns = [c.lower() for c in self.df.columns]
        self.df = self.df.reset_index(drop=True)

    # -----------------------------------------------------
    # 1️⃣ DeMarker
    # -----------------------------------------------------
    def compute_demarker(self, length=14):

        up = np.maximum(self.df["high"] - self.df["high"].shift(1), 0)
        down = np.maximum(self.df["low"].shift(1) - self.df["low"], 0)

        dem_up = up.rolling(length).mean()
        dem_down = down.rolling(length).mean()

        self.df["dem"] = dem_up / (dem_up + dem_down)

    # -----------------------------------------------------
    # 2️⃣ TD SETUP (TD9)
    # -----------------------------------------------------
    def compute_setups(self):

        self.df["buy_setup"] = 0
        self.df["sell_setup"] = 0

        for i in range(4, len(self.df)):

            if self.df["close"].iloc[i] < self.df["close"].iloc[i - 4]:
                self.df.at[i, "buy_setup"] = self.df["buy_setup"].iloc[i - 1] + 1
            else:
                self.df.at[i, "buy_setup"] = 0

            if self.df["close"].iloc[i] > self.df["close"].iloc[i - 4]:
                self.df.at[i, "sell_setup"] = self.df["sell_setup"].iloc[i - 1] + 1
            else:
                self.df.at[i, "sell_setup"] = 0

        # TD9 exhaustion event
        self.df["td9_buy"] = self.df["buy_setup"] == 9
        self.df["td9_sell"] = self.df["sell_setup"] == 9

    # -----------------------------------------------------
    # 3️⃣ TD COUNTDOWN (TD13)
    # -----------------------------------------------------
    def compute_countdown(self):

        self.df["buy_cd"] = 0
        self.df["sell_cd"] = 0

        for i in range(2, len(self.df)):

            if self.df["close"].iloc[i] <= self.df["low"].iloc[i - 2]:
                self.df.at[i, "buy_cd"] = self.df["buy_cd"].iloc[i - 1] + 1
            else:
                self.df.at[i, "buy_cd"] = self.df["buy_cd"].iloc[i - 1]

            if self.df["close"].iloc[i] >= self.df["high"].iloc[i - 2]:
                self.df.at[i, "sell_cd"] = self.df["sell_cd"].iloc[i - 1] + 1
            else:
                self.df.at[i, "sell_cd"] = self.df["sell_cd"].iloc[i - 1]

        self.df["td13_buy_raw"] = self.df["buy_cd"] >= 13
        self.df["td13_sell_raw"] = self.df["sell_cd"] >= 13

    # -----------------------------------------------------
    # 4️⃣ TD13 VALIDATION (Dem + Volume filter)
    # -----------------------------------------------------
    def validate_td13(self):

        self.df["vol_sma"] = self.df["volume"].rolling(20).mean()
        self.df["vol_drop"] = self.df["volume"] < self.df["vol_sma"]

        self.df["td13_buy"] = (
            self.df["td13_buy_raw"]
            & (self.df["dem"] < 0.3)
            & self.df["vol_drop"]
            & (self.df["low"] <= self.df["close"].shift(8))
        )

        self.df["td13_sell"] = (
            self.df["td13_sell_raw"]
            & (self.df["dem"] > 0.7)
            & self.df["vol_drop"]
            & (self.df["high"] >= self.df["close"].shift(8))
        )

    # -----------------------------------------------------
    # 5️⃣ SIGNAL LIFECYCLE (Age + Status)
    # -----------------------------------------------------
    def compute_lifecycle(self):

        self.df["bar_index"] = np.arange(len(self.df))

        # Event bar tracking
        for col in ["td9_buy", "td9_sell", "td13_buy", "td13_sell"]:

            event_bar = f"{col}_bar"
            last_event_bar = f"last_{col}_bar"
            age_col = f"{col}_age"

            self.df[event_bar] = np.where(
                self.df[col], self.df["bar_index"], np.nan
            )

            self.df[last_event_bar] = self.df[event_bar].ffill()
            self.df[age_col] = self.df["bar_index"] - self.df[last_event_bar]

        # ---- TD13 Classification ----
        def classify_td13(age):
            if pd.isna(age):
                return None
            if age <= 2:
                return "Fresh"
            elif age <= 6:
                return "Active"
            elif age <= 12:
                return "Fading"
            else:
                return None

        # ---- TD9 Classification ----
        def classify_td9(age):
            if pd.isna(age):
                return None
            if age <= 2:
                return "Fresh Exhaustion"
            elif age <= 6:
                return "Active Exhaustion"
            else:
                return None

        self.df["td13_buy_status"] = self.df["td13_buy_age"].apply(classify_td13)
        self.df["td13_sell_status"] = self.df["td13_sell_age"].apply(classify_td13)

        self.df["td9_buy_status"] = self.df["td9_buy_age"].apply(classify_td9)
        self.df["td9_sell_status"] = self.df["td9_sell_age"].apply(classify_td9)

    # -----------------------------------------------------
    # RUN
    # -----------------------------------------------------
    def run(self):

        self.compute_demarker()
        self.compute_setups()
        self.compute_countdown()
        self.validate_td13()
        self.compute_lifecycle()

        return self.df
