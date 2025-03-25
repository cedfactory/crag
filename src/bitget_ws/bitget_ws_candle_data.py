import pandas as pd

class WSCandleData:
    def __init__(self, params):
        """
        params: a list of dicts, e.g.
        [
            {"symbol": "BTC", "timeframe": "1m"},
            {"symbol": "BTC", "timeframe": "1h"},
            {"symbol": "ETH", "timeframe": "1m"},
            {"symbol": "ETH", "timeframe": "1h"},
            ...
        ]
        """
        self.state = {}

        # Build the nested dictionary
        for item in params:
            symbol_key = item["symbol"] + "USDT"
            if symbol_key not in self.state:
                self.state[symbol_key] = {}
            self.state[symbol_key][item["timeframe"]] = None

    def set_value(self, symbol_key, timeframe, df):
        """
        Set the value for a given symbol + timeframe combination.
        symbol: "BTC", "ETH", etc. (without "USDT")
        timeframe: e.g. "1m", "1h"
        df: the DataFrame to store or append
        """
        try:
            if not symbol_key.endswith("USDT"):
                symbol_key += "USDT"

            if symbol_key not in self.state:
                self.state[symbol_key] = {}

            if self.state[symbol_key].get(timeframe) is None:
                self.state[symbol_key][timeframe] = df.iloc[:-1].copy()
            else:
                if len(df) == 2:
                    df = df.iloc[:1]
                    existing_df = self.state[symbol_key][timeframe]
                    self.state[symbol_key][timeframe] = pd.concat([existing_df, df])
                    if len(self.state[symbol_key][timeframe]) > 1000:
                        self.state[symbol_key][timeframe] = self.state[symbol_key][timeframe].tail(1000)
                elif len(df) > 2:
                    exit(834)
        except:
            exit(834)


    def get_value(self, symbol_key, timeframe):
        """
        Get the value for a given symbol + timeframe combination.
        Returns None if not found.
        """
        if not symbol_key.endswith("USDT"):
            symbol_key += "USDT"

        return self.state[symbol_key].get(timeframe)

    def get_ohlcv(self, symbol_key, timeframe, length):
        """
        Get the value for a given symbol + timeframe combination.
        Returns None if not found.
        """
        if not symbol_key.endswith("USDT"):
            symbol_key += "USDT"

        return self.state[symbol_key].get(timeframe).tail(length)
