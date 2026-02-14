import pandas as pd
import numpy as np


class DeMarkEngine:

    TD9_WINDOW = 6      # Bars to keep TD9 active
    TD13_WINDOW = 12    # Bars to keep TD13 active

    def __init__(self, df: pd.DataFrame):

        self.df = df.copy()
        self.df.columns = [c.upper() for c in self.df.columns]

        if "DATETIME" in self.df.columns:
            self.df = self.df.sort_values("DATETIME").reset_index(drop=True)

    # ============================================
    # TD SETUP (TD9)
    # ============================================

    def compute_setups(self):

        self.df["BULL_SETUP"] = 0
        self.df["BEAR_SETUP"] = 0

        for i in range(4, len(self.df)):

            if self.df["CLOSE"].iloc[i] < self.df["CLOSE"].iloc[i - 4]:
                self.df.loc[i, "BULL_SETUP"] = self.df["BULL_SETUP"].iloc[i - 1] + 1
            else:
                self.df.loc[i, "BULL_SETUP"] = 0

            if self.df["CLOSE"].iloc[i] > self.df["CLOSE"].iloc[i - 4]:
                self.df.loc[i, "BEAR_SETUP"] = self.df["BEAR_SETUP"].iloc[i - 1] + 1
            else:
                self.df.loc[i, "BEAR_SETUP"] = 0

        self.df["TD9_BUY"] = self.df["BULL_SETUP"] == 9
        self.df["TD9_SELL"] = self.df["BEAR_SETUP"] == 9

    # ============================================
    # TD COUNTDOWN (TD13)
    # ============================================

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

    # ============================================
    # SIGNAL AGING (Persistence)
    # ============================================

    def compute_signal_aging(self):

        for col in ["TD9_BUY", "TD9_SELL", "TD13_BUY", "TD13_SELL"]:

            bar_col = col + "_BAR"
            age_col = col + "_AGE"
            status_col = col + "_STATUS"

            self.df[bar_col] = np.where(self.df[col], self.df.index, np.nan)
            self.df[bar_col] = self.df[bar_col].ffill()
            self.df[age_col] = self.df.index - self.df[bar_col]

            window = self.TD13_WINDOW if "13" in col else self.TD9_WINDOW

            self.df[status_col] = np.where(
                self.df[age_col] == 0,
                "Fresh",
                np.where(
                    self.df[age_col] <= window,
                    "Active",
                    "Expired"
                )
            )

    # ============================================
    # PUBLIC RUN
    # ============================================

    def run(self):

        self.compute_setups()
        self.compute_countdown()
        self.compute_signal_aging()

        return self.df
