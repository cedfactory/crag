from . import rtdp, rtstr, rtctrl
import pandas as pd

class StrategyBigWill(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

        self.AO_Threshold = 0
        # stochOverBought:  0.7  stochOverSold:  0.4  willOverSold:  -85  willOverBought:  -10
        # self.stochOverBought = 0.8 # Initial
        self.stochOverBought = 0.7
        # self.stochOverSold = 0.2 # Initial
        # self.stochOverSold = 0.3 # test 1
        self.stochOverSold = 0.4 # Changed by 0.4
        # self.willOverSold = -85 # Initial
        self.willOverSold = -90  # Initial
        # self.willOverSold = -80
        self.willOverBought = -10 # Initial
        # self.willOverBought = -20 # or -15

        """
        sl:  0  stochOverBought:  0.7  stochOverSold:  0.4  willOverSold:  -80  willOverBought:  -20
        nb pair:  38
        final_wallet mean:  4129.783947368422
        final_wallet max:  17301.44
        vs_hold_pct mean:  2.42078947368421
        vs_hold_pct max:  18.28
        global_win_rate mean:  0.6805263157894736
        global_win_rate max:  0.75
        total_trades mean:  63.71052631578947
        total_trades max:  74.0
        list pairs:  ['DOGE', 'MATIC', 'XLM', 'ATOM', 'MANA', 'CRV', 'EGLD', 'PEOPLE', 'ENJ', 'ZIL', 'APE', 'ROSE', 'C98', 'STORJ', 'COMP', 'LUNA2', 'LUNC', 'ONE', 'MAGIC', 'ASTR', 'ANKR', 'FET', 'HOOK', 'HBAR', 'COTI', 'LIT', 'TLM', 'HOT', 'HFT', 'ZEC', 'UNFI', 'DAR', 'SFP', 'SKL', 'STMX', 'UMA', 'KEY', 'SLP']
        
        sl:  0  stochOverBought:  0.7  stochOverSold:  0.4  willOverSold:  -80  willOverBought:  -15
        nb pair:  42
        final_wallet mean:  4236.242380952381
        final_wallet max:  15574.82
        vs_hold_pct mean:  2.176190476190476
        vs_hold_pct max:  12.79
        global_win_rate mean:  0.6802380952380952
        global_win_rate max:  0.75
        total_trades mean:  57.61904761904762
        total_trades max:  69.0
        list pairs:  ['LINK', 'MATIC', 'ICP', 'XLM', 'AVAX', 'MANA', 'CRV', 'EGLD', 'PEOPLE', 'ENJ', 'ZIL', 'APE', 'XMR', 'ROSE', 'C98', 'STORJ', 'COMP', 'LUNA2', 'LUNC', 'ONE', 'BAT', 'MAGIC', 'ANKR', 'FET', 'RNDR', 'HOOK', 'HBAR', 'COTI', 'VET', 'LIT', 'TLM', 'HOT', 'HFT', 'ZEC', 'UNFI', 'DAR', 'SFP', 'SKL', 'UMA', 'DENT', 'KEY', 'SLP']
        """

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols

        ds.fdp_features = { "ao" : {"indicator": "ao", "ao_window_1": 6, "ao_window_2": 22, "window_size": 22},
                            "ema100" : {"indicator": "ema", "id": "100", "window_size": 100},
                            "ema200": {"indicator": "ema", "id": "200", "window_size": 200},
                            "stoch_rsi": {"indicator": "stoch_rsi", "window_size": 30, "stoch_rsi_window_size":14},
                            "willr" : {"indicator": "willr", "window_size": 14},
                            "postprocess1": {"indicator": "shift", "window_size": 1, "n": "1", "input": ["ao"]}
                            }

        ds.features = self.get_feature_from_fdp_features(ds.fdp_features)
        ds.interval = self.strategy_interval
        print("startegy: ", self.get_info())
        print("strategy features: ", ds.features)

        return ds

    def get_info(self):
        return "StrategyBigWill"

    def condition_for_opening_long_position(self, symbol):
        return self.df_current_data['ao'][symbol] > self.AO_Threshold and \
               self.df_current_data['n1_ao'][symbol] > self.df_current_data['ao'][symbol] and \
               self.df_current_data['willr'][symbol] < self.willOverSold and \
               self.df_current_data['ema_100'][symbol] > self.df_current_data['ema_200'][symbol]

    def condition_for_opening_short_position(self, symbol):
        return self.df_current_data['ao'][symbol] < self.AO_Threshold and \
               self.df_current_data['n1_ao'][symbol] < self.df_current_data['ao'][symbol] and \
               self.df_current_data['willr'][symbol] > self.willOverBought and \
               self.df_current_data['ema_100'][symbol] < self.df_current_data['ema_200'][symbol]

    def condition_for_closing_long_position(self, symbol):
        return (self.df_current_data['ao'][symbol] < self.AO_Threshold
                and self.df_current_data['stoch_rsi'][symbol] > self.stochOverSold) \
               or self.df_current_data['willr'][symbol] > self.willOverBought

    def condition_for_closing_short_position(self, symbol):
        return (self.df_current_data['ao'][symbol] > self.AO_Threshold
                and self.df_current_data['stoch_rsi'][symbol] < self.stochOverBought) \
               or self.df_current_data['willr'][symbol] < self.willOverSold

    def sort_list_symbols(self, lst_symbols):
        print("symbol list: ", lst_symbols)
        df = pd.DataFrame(index=lst_symbols, columns=['ao'])
        for symbol in lst_symbols:
            df.at[symbol, 'ao'] = self.df_current_data['ao'][symbol]
        df.sort_values(by=['ao'], inplace=True, ascending=False)
        lst_symbols = df.index.to_list()
        print("sorted symbols with AO: ", lst_symbols)
        return lst_symbols