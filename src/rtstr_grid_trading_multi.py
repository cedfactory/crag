import pandas as pd
import os
from . import rtdp, rtstr, rtctrl

class StrategyGridTradingMulti(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.MAX_POSITION = 100    # Asset Overall Percent Size

        self.rtctrl = rtctrl.rtctrl(params=params)

        self.zero_print = True

        # Strategy Specifics
        self.list_symbols = []
        self.df_grid_multi = pd.DataFrame(columns=['symbol', 'grid', 'previous_zone_position', 'zone_position'])
        self.df_selling_limits = pd.DataFrame(columns=['symbol', 'selling_limits'])
        self.limit_sell = 1
        self.params = params

        self.grid = 0

        self.share_size = 10
        self.global_tp = 10000
        self.df_size_grid_params = pd.DataFrame()
        if params:
            self.share_size = params.get("share_size", self.share_size)
            self.global_tp = params.get("global_tp", self.global_tp)
            self.df_size_grid_params = params.get("grid_df_params", self.df_size_grid_params)

        if self.global_tp == 0:
            self.global_tp = 10000
        self.net_size = 0.0
        self.global_tp_net = -1000
        self.tp_sl_abort = False

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = [
            "BTC/USD",
            "ETH/USD",
            "XRP/USD",
            "BNB/USD",
            "SOL/USD"
        ]
        ds.features = { "close" : None }
        self.list_symbols = ds.symbols
        return ds

    def log_current_info(self):
        csvfilename = "df_grid.csv"
        self.grid.df_grid.to_csv(csvfilename, sep=',')
        self.log(msg="> df_grid", header="StrategyGridTrading::log_current_info", attachments=[csvfilename])
        os.remove(csvfilename)

        currentdatafilename = "df_current_data.csv"
        self.df_current_data.to_csv(currentdatafilename, sep=',')
        self.log(msg="> df_current_data", header="StrategyGridTrading::log_current_info", attachments=[currentdatafilename])
        os.remove(currentdatafilename)

    def get_info(self):
        return "StrategyGridTrading", self.str_sl, self.str_tp

    def condition_for_buying(self, symbol):
        if self.tp_sl_abort:
            return False

        if len(self.df_grid_multi) == 0:
            self.set_df_multi()

        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        if self.grid.get_zone_position(self.df_current_data['close'][symbol]) == -1:
            return False

        self.previous_zone_position = self.grid.get_previous_zone_position()
        self.zone_position = self.grid.get_zone_position(self.df_current_data['close'][symbol])

        if ((self.zone_position > self.previous_zone_position)
                and (not(self.grid.zone_buy_engaged(self.df_current_data['close'][symbol]))) \
                and (self.previous_zone_position != -1)):
            buying_signal = True
        else:
            buying_signal = False

        self.grid.set_previous_zone_position(self.df_current_data['close'][symbol])
        return buying_signal

    def set_zone_engaged(self, symbol, price):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        self.grid.set_zone_engaged(price)

    def set_lower_zone_unengaged_position(self, symbol, zone_position):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        self.grid.set_lower_zone_unengaged_position(zone_position)

    def condition_for_selling(self, symbol, df_sl_tp):
        if self.tp_sl_abort:
            return True

        if len(self.df_grid_multi) == 0:
            self.set_df_multi()

        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        if self.grid.get_zone_position(self.df_current_data['close'][symbol]) == -1:
            return False

        self.previous_zone_position = self.grid.get_previous_zone_position()
        self.zone_position = self.grid.get_zone_position(self.df_current_data['close'][symbol])

        if ((self.zone_position < self.previous_zone_position)
            and (self.grid.lower_zone_buy_engaged(self.df_current_data['close'][symbol]))
            and (self.previous_zone_position != -1)) \
                or ((isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] > self.TP)
                    or (isinstance(df_sl_tp, pd.DataFrame) and df_sl_tp['roi_sl_tp'][symbol] < self.SL)):
            selling_signal = True
        else:
            selling_signal = False

        if self.rtctrl.wallet_value >= self.rtctrl.init_cash_value + self.rtctrl.init_cash_value * self.global_tp / 100:
            self.global_tp = (self.rtctrl.wallet_value - self.rtctrl.init_cash_value) * 100 / self.rtctrl.init_cash_value
            self.global_tp_net = self.global_tp - self.net_size
            print("global_tp: ", round(self.global_tp, 2), " net_tp: ", round(self.global_tp_net, 2), "protfolio: $", self.rtctrl.wallet_value)

        if self.rtctrl.wallet_value <= self.rtctrl.init_cash_value + self.rtctrl.init_cash_value * self.global_tp_net / 100:
            self.tp_sl_abort = True
            selling_signal = True
            print("abort: $", self.rtctrl.wallet_value)

        return selling_signal


    def get_symbol_buying_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0, 0

        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        available_cash = self.rtctrl.wallet_cash
        if available_cash == 0:
            return 0, 0, 0

        init_cash_value = self.rtctrl.init_cash_value
        # init_cash_value = init_cash_value - init_cash_value * 0.007  # CEDE: replace 0.007 with call to broker fees
        grid_size = self.grid.grid_size
        # CEDE Test: buying_size_value = init_cash_value / grid_size
        buying_size_value = init_cash_value / self.share_size
        # CEDE: Price % based on upper limit. Safer approach
        # CEDE: Always buying / selling the same % size
        # CEDE: The other solution is to buy / sell the same $ amount
        buying_size_percent = buying_size_value * 100 / self.grid.UpperPriceLimit
        buying_size_value = buying_size_percent * self.rtctrl.prices_symbols[symbol] / 100

        wallet_value = available_cash

        # cash_to_buy = wallet_value * self.SPLIT / 100
        cash_to_buy = buying_size_value

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        percent = cash_to_buy * 100 / wallet_value
        min_size = self.df_size_grid_params.loc[self.df_size_grid_params['symbol'] == symbol, 'balance_min_size'].iloc[0]
        if size < min_size:
            size = min_size
        return size, percent, self.grid.get_zone_position(self.rtctrl.prices_symbols[symbol])

    def get_symbol_selling_size(self, symbol):
        size, percent, zone = self.get_symbol_buying_size(symbol)

        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        zone = self.grid.get_lower_zone_buy_engaged(zone)
        return size, percent, zone

    def get_lower_zone_buy_engaged(self, symbol):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        zone = self.grid.get_zone_position(self.rtctrl.prices_symbols[symbol])
        return self.grid.get_lower_zone_buy_engaged(zone)

    def reset_selling_limits(self):
        self.df_selling_limits["selling_limits"] = self.limit_sell

    def set_selling_limits(self, df_selling_symbols):
        for symbol in df_selling_symbols.symbol.tolist():
            self.df_selling_limits.loc[self.df_selling_limits['symbol'] == symbol, "selling_limits"] = 0

    def force_selling_limits(self):
        self.df_selling_limits["selling_limits"] = self.limit_sell + 1

    def count_selling_limits(self, symbol):
        self.df_selling_limits.loc[self.df_selling_limits['symbol'] == symbol, "selling_limits"] = \
            self.df_selling_limits.loc[self.df_selling_limits['symbol'] == symbol, "selling_limits"].iloc[0] + 1

    def get_selling_limit(self, symbol):
        if self.df_selling_limits.loc[self.df_selling_limits['symbol'] == symbol, "selling_limits"].iloc[0] < 1:
            return True
        else:
            return False

    def set_df_multi(self):
        self.df_selling_limits['symbol'] = self.list_symbols
        self.df_selling_limits['selling_limits'] = 0

        self.df_grid_multi = pd.DataFrame(columns=['symbol', 'grid', 'previous_zone_position', 'zone_position'])
        self.df_grid_multi['symbol'] = self.list_symbols
        self.df_grid_multi['previous_zone_position'] = 0
        self.df_grid_multi['zone_position'] = 0

        for symbol in self.list_symbols:
            self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"] = GridLevelPosition(symbol, params=self.params)

class GridLevelPosition():
    def __init__(self, symbol, params=None):
        self.UpperPriceLimit = 20500
        self.grid_step = .5 # percent
        self.grid_threshold = 0
        self.df_grid_params = pd.DataFrame()
        if params:
            self.df_grid_params = params.get("grid_df_params", self.df_grid_params)

        self.UpperPriceLimit = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'UpperPriceLimit'].iloc[0]
        self.LowerPriceLimit = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'LowerPriceLimit'].iloc[0]
        self.OutboundZoneMax = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'OutboundZoneMax'].iloc[0]
        self.OutboundZoneMin = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'OutboundZoneMin'].iloc[0]
        self.grid_step = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'grid_step'].iloc[0]
        self.grid_threshold = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'grid_threshold'].iloc[0]

        GridLen = self.UpperPriceLimit - self.LowerPriceLimit
        GridStep = GridLen * self.grid_step / 100

        # CEDE Comment: Alternative solution
        # GridLen = self.UpperPriceLimit
        # if GridLen * self.grid_step > 1:
            # GridStep = int(GridLen * self.grid_step / 100)

        zone_limit = self.LowerPriceLimit
        lst_zone_limit = []

        while(zone_limit < self.UpperPriceLimit):
            lst_zone_limit.append(zone_limit)
            zone_limit = zone_limit + GridStep
        lst_zone_limit.append(self.UpperPriceLimit)

        lst_start_zone = lst_zone_limit.copy()
        lst_start_zone.insert(0, self.OutboundZoneMin)

        lst_end_zone = lst_zone_limit.copy()
        lst_end_zone.append(self.OutboundZoneMax)

        lst_zone_id = ['zone_' + str(i) for i in range(len(lst_start_zone))]

        lst_start_zone.reverse()
        lst_end_zone.reverse()

        self.df_grid = pd.DataFrame()
        self.df_grid['zone_id'] = lst_zone_id
        self.df_grid['start'] = lst_start_zone
        self.df_grid['end'] = lst_end_zone

        self.df_grid['start'] = self.df_grid['start'] + self.df_grid['start'] * self.grid_threshold / 100
        self.df_grid['end'] = self.df_grid['end'] - self.df_grid['end'] * self.grid_threshold / 100

        self.df_grid['previous_position'] = 0
        self.df_grid['actual_position'] = 0
        self.df_grid['zone_engaged'] = False
        self.df_grid['buying_value'] = 0

        self.grid_size = len(self.df_grid)

    def get_zone_position(self, price):
        try:
            start_debug = (self.df_grid['start']) < price
            end_debug = ( (self.df_grid['end']) >= price)
            if len(self.df_grid[( (self.df_grid['start']) < price)
                                & ( (self.df_grid['end']) >= price)]) > 0:
                zone = self.df_grid[( (self.df_grid['start']) < price)
                                    & ( (self.df_grid['end']) >= price)].index[0]
            else:
                zone = -1
        except:
            zone = -1
        return zone

    def get_previous_zone_position(self):
        if len(self.df_grid[(self.df_grid['previous_position'] != 0)].index) == 0:
            previous_zone_position = -1
        else:
            previous_zone_position = self.df_grid[(self.df_grid['previous_position'] != 0)].index[0]
        return previous_zone_position

    def set_previous_zone_position(self, price):
        self.df_grid['previous_position'] = 0
        zone_position = self.get_zone_position(price)
        self.df_grid.loc[zone_position, 'previous_position'] = 1

    def set_zone_engaged(self, price):
        zone_position = self.get_zone_position(price)

        self.df_grid.loc[zone_position, 'zone_engaged'] = True
        self.df_grid.loc[zone_position, 'buying_value'] = price

    def set_lower_zone_unengaged(self, price):
        zone_position = self.get_zone_position(price)

        self.df_grid.loc[zone_position, 'zone_engaged'] = False
        self.df_grid.loc[zone_position, 'buying_value'] = 0

    def set_lower_zone_unengaged_position(self, zone_position):
        self.df_grid.loc[zone_position, 'zone_engaged'] = False
        self.df_grid.loc[zone_position, 'buying_value'] = 0

    def zone_buy_engaged(self, price):
        zone_position = self.get_zone_position(price)
        return self.df_grid.loc[zone_position, 'zone_engaged']

    def lower_zone_buy_engaged(self, price):
        zone_position = self.get_zone_position(price)
        engaged_zone_position = self.get_lower_zone_buy_engaged(zone_position)
        return engaged_zone_position != -1

        # return self.df_grid.loc[zone_position-1, 'zone_engaged']

    def get_lower_zone_buy_engaged(self, zone_position):
        first_lower_position = -1

        df_grid = self.df_grid.copy()
        df_grid = df_grid.iloc[zone_position+1:,:]
        lower_position_exist = df_grid['zone_engaged'].sum()

        if lower_position_exist > 0:
            # first_lower_position = df_grid[(self.df_grid['zone_engaged'])].index[0]
            df2 = df_grid.loc[(self.df_grid['zone_engaged'])]
            first_lower_position = df2.zone_engaged.isnull().index[0]
        return first_lower_position

    def get_UpperPriceLimit(self):
        return self.UpperPriceLimit
