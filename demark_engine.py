import pandas as pd
import numpy as np


class DeMarkEngine:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

        # Standardize column names
        self.df.columns = [c.lower() for c in self.df.columns]

        required = ["open", "high", "low", "close", "volume"]
        for col in required:
            if col not in self.df.columns:
                raise ValueError(f"Missing required column: {col}")

    # =====================================================
    # 1️⃣ DeMarker (Aligned with Pine)
    # =====================================================

    def compute_demarker(self, length=14):
        up = np.maximum(self.df["high"] - self.df["high"].shift(1), 0)
        down = np.maximum(self.df["low"].shift(1) - self.df["low"], 0)

        dem_up = up.rolling(length).mean()
        dem_down = down.rolling(length).mean()

        self.df["dem"] = dem_up / (dem_up + dem_down)

    # =====================================================
    # 2️⃣ TD Setup
    # =====================================================

    def compute_setups(self):
        self.df["buy_setup"] = 0
        self.df["sell_setup"] = 0

        for i in range(4, len(self.df)):
            if self.df["close"].iloc[i] < self.df["close"].iloc[i - 4]:
                self.df.at[self.df.index[i], "buy_setup"] = \
                    self.df["buy_setup"].iloc[i - 1] + 1
            else:
                self.df.at[self.df.index[i], "buy_setup"] = 0

            if self.df["close"].iloc[i] > self.df["close"].iloc[i - 4]:
                self.df.at[self.df.index[i], "sell_setup"] = \
                    self.df["sell_setup"].iloc[i - 1] + 1
            else:
                self.df.at[self.df.index[i], "sell_setup"] = 0

        # Perfected Setup
        self.df["perfect_buy"] = (
            (self.df["buy_setup"] == 9) &
            (
                (self.df["high"].shift(8) > self.df["high"].shift(6)) |
                (self.df["high"].shift(8) > self.df["high"].shift(7))
            )
        )

        self.df["perfect_sell"] = (
            (self.df["sell_setup"] == 9) &
            (
                (self.df["low"].shift(8) < self.df["low"].shift(6)) |
                (self.df["low"].shift(8) < self.df["low"].shift(7))
            )
        )

    # =====================================================
    # 3️⃣ Countdown with Cancellation + Recycling
    # =====================================================

    def compute_countdown(self):
        self.df["buy_cd"] = 0
        self.df["sell_cd"] = 0

        for i in range(2, len(self.df)):

            # Cancellation
            if self.df["sell_setup"].iloc[i] >= 1:
                buy_prev = 0
            else:
                buy_prev = self.df["buy_cd"].iloc[i - 1]

            if self.df["buy_setup"].iloc[i] >= 1:
                sell_prev = 0
            else:
                sell_prev = self.df["sell_cd"].iloc[i - 1]

            # Countdown conditions
            if self.df["close"].iloc[i] < self.df["close"].iloc[i - 2]:
                self.df.at[self.df.index[i], "buy_cd"] = buy_prev + 1
            else:
                self.df.at[self.df.index[i], "buy_cd"] = buy_prev

            if self.df["close"].iloc[i] > self.df["close"].iloc[i - 2]:
                self.df.at[self.df.index[i], "sell_cd"] = sell_prev + 1
            else:
                self.df.at[self.df.index[i], "sell_cd"] = sell_prev

            # Recycling
            if self.df["buy_setup"].iloc[i] >= 9 and self.df["buy_cd"].iloc[i] < 13:
                self.df.at[self.df.index[i], "buy_cd"] = 0

            if self.df["sell_setup"].iloc[i] >= 9 and self.df["sell_cd"].iloc[i] < 13:
                self.df.at[self.df.index[i], "sell_cd"] = 0

    # =====================================================
    # 4️⃣ TD13 Validation (Volume + DeMarker + Deferral)
    # =====================================================

    def validate_td13(self):
        self.df["vol_sma"] = self.df["volume"].rolling(20).mean()
        self.df["vol_drop"] = self.df["volume"] < self.df["vol_sma"]

        self.df["valid_buy_13"] = (
            (self.df["buy_cd"] >= 13) &
            (self.df["dem"] < 0.3) &
            (self.df["vol_drop"]) &
            (self.df["low"] <= self.df["close"].shift(8))
        )

        self.df["valid_sell_13"] = (
            (self.df["sell_cd"] >= 13) &
            (self.df["dem"] > 0.7) &
            (self.df["vol_drop"]) &
            (self.df["high"] >= self.df["close"].shift(8))
        )

    # =====================================================
    # 5️⃣ Signal Aging
    # =====================================================

    def compute_signal_age(self):
        self.df["buy_signal_bar"] = np.where(self.df["valid_buy_13"], np.arange(len(self.df)), np.nan)
        self.df["sell_signal_bar"] = np.where(self.df["valid_sell_13"], np.arange(len(self.df)), np.nan)

        self.df["buy_signal_bar"] = self.df["buy_signal_bar"].ffill()
        self.df["sell_signal_bar"] = self.df["sell_signal_bar"].ffill()

        self.df["buy_age"] = np.arange(len(self.df)) - self.df["buy_signal_bar"]
        self.df["sell_age"] = np.arange(len(self.df)) - self.df["sell_signal_bar"]

        def classify(age):
            if np.isnan(age):
                return "Neutral"
            if age <= 4:
                return "Fresh"
            if age <= 12:
                return "Fading"
            return "Expired"

        self.df["buy_status"] = self.df["buy_age"].apply(classify)
        self.df["sell_status"] = self.df["sell_age"].apply(classify)

    # =====================================================
    # 6️⃣ Alignment Score
    # =====================================================

    def compute_alignment_score(self):
        self.df["alignment_score"] = (
            self.df["perfect_buy"].astype(int) +
            self.df["perfect_sell"].astype(int) +
            self.df["valid_buy_13"].astype(int) * 2 +
            self.df["valid_sell_13"].astype(int) * 2
        )

    # =====================================================
    # RUN ENGINE
    # =====================================================

    def run(self):
        self.compute_demarker()
        self.compute_setups()
        self.compute_countdown()
        self.validate_td13()
        self.compute_signal_age()
        self.compute_alignment_score()

        # Standard naming for scanner
        self.df["bull_setup"] = self.df["buy_setup"]
        self.df["bear_setup"] = self.df["sell_setup"]
        self.df["bull_countdown"] = self.df["buy_cd"]
        self.df["bear_countdown"] = self.df["sell_cd"]

        return self.df
