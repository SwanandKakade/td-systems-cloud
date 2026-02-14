import pandas as pd
import numpy as np


class DeMarkEngine:

    def __init__(self, df: pd.DataFrame):
        """
        Expected columns:
        DATETIME, OPEN, HIGH, LOW, CLOSE, VOLUME
        """

        self.df = df.copy()
        self.df.columns = [c.upper() for c in self.df.columns]

        # Ensure sorted
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
    # 2️⃣ TD SETUP (9)
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

        # Perfected Setup (Pine-aligned logic)
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

    # =====================================================
    # 3️⃣ TD COUNTDOWN (13)
    # =====================================================

    def compute_countdown(self):

        self.df["BULL_CD"] = 0
        self.df["BEAR_CD"] = 0

        bull_cd = 0
        bear_cd = 0

        for i in range(2, len(self.df)):

            # Reset on opposite setup
            if self.df["BULL_SETUP"].iloc[i] >= 9:
                bull_cd = 0
            if self.df["BEAR_SETUP"].iloc[i] >= 9:
                bear_cd = 0

            # Bullish Countdown condition (close <= low[2])
            if self.df["CLOSE"].iloc[i] <= self.df["LOW"].iloc[i - 2]:
                bull_cd += 1

            # Bearish Countdown condition (close >= high[2])
            if self.df["CLOSE"].iloc[i] >= self.df["HIGH"].iloc[i - 2]:
                bear_cd += 1

            self.df.loc[i, "BULL_CD"] = bull_cd
            self.df.loc[i, "BEAR_CD"] = bear_cd

    # =====================================================
    # 4️⃣ TD13 VALIDATION
    # =====================================================

    def validate_td13(self):

        # Volume filter
        self.df["VOL_SMA"] = self.df["VOLUME"].rolling(20).mean()
        self.df["VOL_DROP"] = self.df["VOLUME"] < self.df["VOL_SMA"]

        # Valid Buy 13
        self.df["VALID_BUY_13"] = (
            (self.df["BULL_CD"] >= 13) &
            (self.df["DEMARKER"] < 0.3) &
            (self.df["VOL_DROP"]) &
            (self.df["LOW"] <= self.df["CLOSE"].shift(8))
        )

        # Valid Sell 13
        self.df["VALID_SELL_13"] = (
            (self.df["BEAR_CD"] >= 13) &
            (self.df["DEMARKER"] > 0.7) &
            (self.df["VOL_DROP"]) &
            (self.df["HIGH"] >= self.df["CLOSE"].shift(8))
        )

    # =====================================================
    # 5️⃣ SIGNAL AGING
    # =====================================================

    def compute_signal_aging(self):

        self.df["BUY_SIGNAL_BAR"] = np.where(self.df["VALID_BUY_13"], self.df.index, np.nan)
        self.df["SELL_SIGNAL_BAR"] = np.where(self.df["VALID_SELL_13"], self.df.index, np.nan)

        self.df["BUY_SIGNAL_BAR"] = self.df["BUY_SIGNAL_BAR"].ffill()
        self.df["SELL_SIGNAL_BAR"] = self.df["SELL_SIGNAL_BAR"].ffill()

        self.df["BUY_AGE"] = self.df.index - self.df["BUY_SIGNAL_BAR"]
        self.df["SELL_AGE"] = self.df.index - self.df["SELL_SIGNAL_BAR"]

        # Age classification
        self.df["BUY_STATUS"] = np.where(
            self.df["VALID_BUY_13"],
            "Fresh",
            np.where(
                (self.df["BUY_AGE"] > 0) & (self.df["BUY_AGE"] <= 10),
                "Active",
                "Expired"
            )
        )

        self.df["SELL_STATUS"] = np.where(
            self.df["VALID_SELL_13"],
            "Fresh",
            np.where(
                (self.df["SELL_AGE"] > 0) & (self.df["SELL_AGE"] <= 10),
                "Active",
                "Expired"
            )
        )

    # =====================================================
    # 6️⃣ PUBLIC RUN
    # =====================================================

    def run(self):

        self.compute_demarker()
        self.compute_setups()
        self.compute_countdown()
        self.validate_td13()
        self.compute_signal_aging()

        # Standardized lowercase mapping (for main.py simplicity)
        self.df["bull_setup"] = self.df["BULL_SETUP"]
        self.df["bear_setup"] = self.df["BEAR_SETUP"]
        self.df["perfect_buy"] = self.df["PERFECT_BUY"]
        self.df["perfect_sell"] = self.df["PERFECT_SELL"]
        self.df["bull_countdown"] = self.df["BULL_CD"]
        self.df["bear_countdown"] = self.df["BEAR_CD"]
        self.df["valid_buy_13"] = self.df["VALID_BUY_13"]
        self.df["valid_sell_13"] = self.df["VALID_SELL_13"]
        self.df["buy_status"] = self.df["BUY_STATUS"]
        self.df["sell_status"] = self.df["SELL_STATUS"]
        self.df["buy_age"] = self.df["BUY_AGE"]
        self.df["sell_age"] = self.df["SELL_AGE"]

        return self.df
