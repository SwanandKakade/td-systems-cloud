import pandas as pd
import numpy as np


class DeMarkEngine:

    TD9_WINDOW = 4
    TD13_WINDOW = 8

    def __init__(self, df: pd.DataFrame):
        """
        Expected columns:
        DATETIME, OPEN, HIGH, LOW, CLOSE, VOLUME
        """

        self.df = df.copy()
        self.df.columns = [c.upper() for c in self.df.columns]

        if "DATETIME" in self.df.columns:
            self.df = self.df.sort_values("DATETIME").reset_index(drop=True)

    # =====================================================
    # 1️⃣ DEMARKER
    # =====================================================

    def compute_demarker(self, length: int = 14):

        up = np.maximum(self.df["HIGH"] - self.df["HIGH"].shift(1), 0)
        down = np.maximum(self.df["LOW"].shift(1) - self.df["LOW"], 0)

        dem_up = up.rolling(length).mean()
        dem_down = down.rolling(length).mean()

        self.df["DEMARKER"] = dem_up / (dem_up + dem_down)

    # =====================================================
    # 2️⃣ TD SETUP (TD9)
    # =====================================================

    def compute_setups(self):

        self.df["BULL_SETUP"] = 0
        self.df["BEAR_SETUP"] = 0

        for i in range(4, len(self.df)):

            # Buy Setup
            if self.df["CLOSE"].iloc[i] < self.df["CLOSE"].iloc[i - 4]:
                self.df.loc[i, "BULL_SETUP"] = self.df["BULL_SETUP"].iloc[i - 1] + 1
            else:
                self.df.loc[i, "BULL_SETUP"] = 0

            # Sell Setup
            if self.df["CLOSE"].iloc[i] > self.df["CLOSE"].iloc[i - 4]:
                self.df.loc[i, "BEAR_SETUP"] = self.df["BEAR_SETUP"].iloc[i - 1] + 1
            else:
                self.df.loc[i, "BEAR_SETUP"] = 0

        # Perfected setups
        self.df["PERFECT_BUY"] = (
            (self.df["BULL_SETUP"] == 9) &
            (
                (self.df["HIGH"].shift(8) > self.df["HIGH"].shift(6)) |
                (self.df["HIGH"].shift(8) > self.df["HIGH"].shift(7))
            )
        )

        self.df["PERFECT_SELL"] = (
            (self.df["BEAR_SETUP"] == 9) &
            (
                (self.df["LOW"].shift(8) < self.df["LOW"].shift(6)) |
                (self.df["LOW"].shift(8) < self.df["LOW"].shift(7))
            )
        )

        # TD9 exhaustion flag
        self.df["TD9_BUY"] = self.df["BULL_SETUP"] == 9
        self.df["TD9_SELL"] = self.df["BEAR_SETUP"] == 9

    # =====================================================
    # 3️⃣ TD COUNTDOWN (TD13)
    # =====================================================

    def compute_countdown(self):

        self.df["BULL_CD"] = 0
        self.df["BEAR_CD"] = 0

        bull_cd = 0
        bear_cd = 0

        for i in range(2, len(self.df)):

            if self.df["BULL_SETUP"].iloc[i] >= 9:
                bull_cd = 0
            if self.df["BEAR_SETUP"].iloc[i] >= 9:
                bear_cd = 0

            if self.df["CLOSE"].iloc[i] <= self.df["LOW"].iloc[i - 2]:
                bull_cd += 1

            if self.df["CLOSE"].iloc[i] >= self.df["HIGH"].iloc[i - 2]:
                bear_cd += 1

            self.df.loc[i, "BULL_CD"] = bull_cd
            self.df.loc[i, "BEAR_CD"] = bear_cd

        self.df["TD13_BUY"] = self.df["BULL_CD"] >= 13
        self.df["TD13_SELL"] = self.df["BEAR_CD"] >= 13

    # =====================================================
    # 4️⃣ SIGNAL AGING (CRITICAL)
    # =====================================================

    def compute_signal_aging(self):

        # Store signal bar index
        self.df["TD9_BUY_BAR"] = np.where(self.df["TD9_BUY"], self.df.index, np.nan)
        self.df["TD9_SELL_BAR"] = np.where(self.df["TD9_SELL"], self.df.index, np.nan)

        self.df["TD13_BUY_BAR"] = np.where(self.df["TD13_BUY"], self.df.index, np.nan)
        self.df["TD13_SELL_BAR"] = np.where(self.df["TD13_SELL"], self.df.index, np.nan)

        # Forward fill to track persistence
        self.df["TD9_BUY_BAR"] = self.df["TD9_BUY_BAR"].ffill()
        self.df["TD9_SELL_BAR"] = self.df["TD9_SELL_BAR"].ffill()
        self.df["TD13_BUY_BAR"] = self.df["TD13_BUY_BAR"].ffill()
        self.df["TD13_SELL_BAR"] = self.df["TD13_SELL_BAR"].ffill()

        # Calculate age
        self.df["TD9_BUY_AGE"] = self.df.index - self.df["TD9_BUY_BAR"]
        self.df["TD9_SELL_AGE"] = self.df.index - self.df["TD9_SELL_BAR"]

        self.df["TD13_BUY_AGE"] = self.df.index - self.df["TD13_BUY_BAR"]
        self.df["TD13_SELL_AGE"] = self.df.index - self.df["TD13_SELL_BAR"]

        # TD9 classification
        self.df["TD9_BUY_STATUS"] = np.where(
            self.df["TD9_BUY_AGE"] == 0,
            "Fresh",
            np.where(
                self.df["TD9_BUY_AGE"] <= self.TD9_WINDOW,
                "Active",
                "Expired"
            )
        )

        self.df["TD9_SELL_STATUS"] = np.where(
            self.df["TD9_SELL_AGE"] == 0,
            "Fresh",
            np.where(
                self.df["TD9_SELL_AGE"] <= self.TD9_WINDOW,
                "Active",
                "Expired"
            )
        )

        # TD13 classification
        self.df["TD13_BUY_STATUS"] = np.where(
            self.df["TD13_BUY_AGE"] == 0,
            "Fresh",
            np.where(
                self.df["TD13_BUY_AGE"] <= self.TD13_WINDOW,
                "Active",
                "Expired"
            )
        )

        self.df["TD13_SELL_STATUS"] = np.where(
            self.df["TD13_SELL_AGE"] == 0,
            "Fresh",
            np.where(
                self.df["TD13_SELL_AGE"] <= self.TD13_WINDOW,
                "Active",
                "Expired"
            )
        )

    # =====================================================
    # 5️⃣ RUN ENGINE
    # =====================================================

    def run(self):

        self.compute_demarker()
        self.compute_setups()
        self.compute_countdown()
        self.compute_signal_aging()

        # Lowercase mapping for main.py
        self.df["bull_setup"] = self.df["BULL_SETUP"]
        self.df["bear_setup"] = self.df["BEAR_SETUP"]

        self.df["perfect_buy"] = self.df["PERFECT_BUY"]
        self.df["perfect_sell"] = self.df["PERFECT_SELL"]

        self.df["td9_buy_status"] = self.df["TD9_BUY_STATUS"]
        self.df["td9_sell_status"] = self.df["TD9_SELL_STATUS"]

        self.df["td13_buy_status"] = self.df["TD13_BUY_STATUS"]
        self.df["td13_sell_status"] = self.df["TD13_SELL_STATUS"]

        self.df["td9_buy_age"] = self.df["TD9_BUY_AGE"]
        self.df["td9_sell_age"] = self.df["TD9_SELL_AGE"]

        self.df["td13_buy_age"] = self.df["TD13_BUY_AGE"]
        self.df["td13_sell_age"] = self.df["TD13_SELL_AGE"]

        return self.df
