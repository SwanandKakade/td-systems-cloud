import pandas as pd
import numpy as np


class DeMarkEngine:

    def __init__(self, df):
        self.df = df.copy()

    def compute_demarker(self, length=14):
        up = np.maximum(self.df["high"] - self.df["high"].shift(1), 0)
        down = np.maximum(self.df["low"].shift(1) - self.df["low"], 0)

        dem_up = up.rolling(length).mean()
        dem_down = down.rolling(length).mean()

        self.df["dem"] = dem_up / (dem_up + dem_down)

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

    def compute_countdown(self):
        self.df["buy_cd"] = 0
        self.df["sell_cd"] = 0

        for i in range(2, len(self.df)):
            if self.df["close"].iloc[i] < self.df["close"].iloc[i - 2]:
                self.df.at[self.df.index[i], "buy_cd"] = \
                    self.df["buy_cd"].iloc[i - 1] + 1
            else:
                self.df.at[self.df.index[i], "buy_cd"] = self.df["buy_cd"].iloc[i - 1]

            if self.df["close"].iloc[i] > self.df["close"].iloc[i - 2]:
                self.df.at[self.df.index[i], "sell_cd"] = \
                    self.df["sell_cd"].iloc[i - 1] + 1
            else:
                self.df.at[self.df.index[i], "sell_cd"] = self.df["sell_cd"].iloc[i - 1]

    def validate_td13(self):
        self.df["vol_sma"] = self.df["volume"].rolling(20).mean()
        self.df["vol_drop"] = self.df["volume"] < self.df["vol_sma"]

        self.df["valid_buy"] = (
            (self.df["buy_cd"] >= 13) &
            (self.df["dem"] < 0.3) &
            (self.df["vol_drop"]) &
            (self.df["low"] <= self.df["close"].shift(8))
        )

        self.df["valid_sell"] = (
            (self.df["sell_cd"] >= 13) &
            (self.df["dem"] > 0.7) &
            (self.df["vol_drop"]) &
            (self.df["high"] >= self.df["close"].shift(8))
        )

    def run(self):
        self.compute_demarker()
        self.compute_setups()
        self.compute_countdown()
        self.validate_td13()

        # 🔥 Standardized column naming (IMPORTANT)
        self.df["bull_setup"] = self.df["buy_setup"]
        self.df["bear_setup"] = self.df["sell_setup"]
        self.df["bull_countdown"] = self.df["buy_cd"]
        self.df["bear_countdown"] = self.df["sell_cd"]
        self.df["valid_buy_13"] = self.df["valid_buy"]
        self.df["valid_sell_13"] = self.df["valid_sell"]

        return self.df
