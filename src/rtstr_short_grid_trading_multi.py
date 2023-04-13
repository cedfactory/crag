import pandas as pd
import os
from . import rtdp, rtstr, rtctrl

class StrategyShortGridTradingMulti(rtstr.RealTimeStrategy):

    def __init__(self, params=None):
        super().__init__(params)

        self.rtctrl = rtctrl.rtctrl(params=params)
        self.rtctrl.set_list_open_position_type(self.get_lst_opening_type())
        self.rtctrl.set_list_close_position_type(self.get_lst_closing_type())

        self.zero_print = True

        # Strategy Specifics
        self.df_grid_multi = pd.DataFrame(columns=['symbol', 'grid', 'previous_zone_position', 'zone_position'])
        self.df_selling_limits = pd.DataFrame(columns=['symbol', 'selling_limits'])
        self.limit_sell = 1
        self.params = params

        self.grid = 0

        self.share_size = 10
        self.df_size_grid_params = pd.DataFrame()
        if params:
            self.share_size = params.get("share_size", self.share_size)
            if isinstance(self.share_size, str):
                self.share_size = int(self.share_size)
            self.df_size_grid_params = params.get("grid_df_params", self.df_size_grid_params)
            if isinstance(self.df_size_grid_params, str):
                config_path = './symbols'
                self.df_size_grid_params = os.path.join(config_path, self.df_size_grid_params)
                self.df_size_grid_params = pd.read_csv(self.df_size_grid_params)

        # add commision 0.08
        self.df_size_grid_params['balance_min_size'] = self.df_size_grid_params['balance_min_size'] / (1 - 0.008)

        if self.share_size == 0:
            self.share_size = 1



        self.net_size = 0.0
        self.set_df_multi()

    def get_data_description(self):
        ds = rtdp.DataDescription()
        ds.symbols = self.lst_symbols
        ds.features = { "close" : None,
                        "sma_30" : 30,
                        "slope_30" : 30}
        return ds

    def log_info(self):
        info = ""
        info += "share_size = {}\n".format(self.share_size)
        info += "symbols = {}\n".format(",".join(self.lst_symbols))
        self.log(msg=info, header="StrategyShortGridTradingMulti::log_info")

    def get_info(self):
        return "StrategyShortGridTrading"

    def is_down_trend(self, symbol):
        return self.df_current_data['slope_30'][symbol] < 0 \
               and self.df_current_data['close'][symbol] < self.df_current_data['sma_30'][symbol]

    def is_up_trend(self, symbol):
        return self.df_current_data['slope_30'][symbol] > 0 \
               and self.df_current_data['close'][symbol] > self.df_current_data['sma_30'][symbol]

    def condition_for_opening_long_position(self, symbol):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]

        if self.grid.exit_range_up_trend(self.df_current_data['close'][symbol]) \
                and self.trade_over_range_limits \
                and not self.is_open_type_short_or_long(symbol) \
                and self.is_up_trend(symbol) \
                and self.grid.is_grid_flushed():
            return True

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

    def condition_for_opening_short_position(self, symbol):
        if self.trade_over_range_limits:
            self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
            if self.grid.exit_range_down_trend(self.df_current_data['close'][symbol])\
                    and not self.is_open_type_short(symbol) \
                    and not self.is_open_type_long(symbol) \
                    and self.is_down_trend(symbol)\
                    and self.grid.is_grid_flushed():
                return True
            else:
                return False
        else:
            return False

    def set_zone_engaged(self, symbol, price):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        self.grid.set_zone_engaged(price)

    def set_lower_zone_unengaged_position(self, symbol, zone_position):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        self.grid.set_lower_zone_unengaged_position(zone_position)

    def condition_for_closing_long_position(self, symbol):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]

        if self.is_specific_grid_open_type_long(symbol) and self.is_down_trend(symbol) and self.grid.is_grid_flushed():
            return True

        if self.grid.exit_range_down_trend(self.df_current_data['close'][symbol])\
                and not self.grid.is_grid_flushed():
            return True
        elif self.grid.exit_range_down_trend(self.df_current_data['close'][symbol])\
                and self.grid.is_grid_flushed():
            return False

        if self.grid.get_zone_position(self.df_current_data['close'][symbol]) == -1:
            return False

        self.previous_zone_position = self.grid.get_previous_zone_position()
        self.zone_position = self.grid.get_zone_position(self.df_current_data['close'][symbol])

        if self.zone_position < self.previous_zone_position \
                and self.grid.lower_zone_buy_engaged(self.df_current_data['close'][symbol]) \
                and self.previous_zone_position != -1:
            selling_signal = True
        else:
            selling_signal = False

        return selling_signal

    def condition_for_closing_short_position(self, symbol):
        result = False
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        if self.is_open_type_short(symbol) and self.is_up_trend(symbol) and self.grid.is_grid_flushed():
            result = True
        return result

    def condition_for_grid_out_of_range_sl_tp_signal(self, symbol, df_sl_tp):
        # SIGNAL SPECIFIC TO GRID STRATEGY
        if not self.is_out_of_range(symbol):
            return False
        result = False
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        if isinstance(df_sl_tp, pd.DataFrame) \
                and self.grid.exit_range_down_trend(self.df_current_data['close'][symbol]) \
                and self.is_open_type_short(symbol) \
                and self.grid.is_grid_flushed() \
                and (df_sl_tp['roi_sl_tp'][symbol] > self.grid.get_out_of_range_TP()
                     or df_sl_tp['roi_sl_tp'][symbol] < self.grid.get_out_of_range_SL()):
            result = True
            print("============= GRID CLOSE SHORT SL_TP =============")
        elif isinstance(df_sl_tp, pd.DataFrame)\
                and self.grid.exit_range_up_trend(self.df_current_data['close'][symbol]) \
                and self.is_specific_grid_open_type_long(symbol) \
                and self.grid.is_grid_flushed() \
                and (df_sl_tp['roi_sl_tp'][symbol] > self.grid.get_out_of_range_TP()
                     or df_sl_tp['roi_sl_tp'][symbol] < self.grid.get_out_of_range_SL()):
            result = True
            print("============= GRID CLOSE LONG SL_TP =============")
        return result

    def get_symbol_buying_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0, 0

        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        available_cash = self.rtctrl.wallet_cash - self.rtctrl.wallet_cash_borrowed
        if available_cash == 0:
            return 0, 0, 0

        init_cash_value = self.rtctrl.init_cash_value

        if (self.grid.exit_range_down_trend(self.df_current_data['close'][symbol])
            or self.grid.exit_range_up_trend(self.df_current_data['close'][symbol])) \
                and self.grid.is_grid_flushed():
            buying_size_value = available_cash * self.MAX_POSITION / 100
        else:
            buying_size_value = init_cash_value * self.MAX_POSITION * self.share_size / 100 / 100

        wallet_value = available_cash
        cash_to_buy = buying_size_value

        if cash_to_buy > available_cash:
            cash_to_buy = available_cash

        size = cash_to_buy / self.rtctrl.prices_symbols[symbol]

        # percent = cash_to_buy * 100 / wallet_value
        percent = cash_to_buy * 100 /  self.rtctrl.wallet_value
        # min_size = self.df_size_grid_params.loc[self.df_size_grid_params['symbol'] == symbol, 'balance_min_size'].iloc[0]

        if self.is_open_type_short(symbol):
            size = -size

        return size, percent, self.grid.get_zone_position(self.rtctrl.prices_symbols[symbol])

    def get_symbol_selling_size(self, symbol):
        if not symbol in self.rtctrl.prices_symbols or self.rtctrl.prices_symbols[symbol] < 0: # first init at -1
            return 0, 0, 0

        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]

        buying_size_value = self.rtctrl.init_cash_value / self.share_size
        size = buying_size_value / self.rtctrl.prices_symbols[symbol]
        percent = self.rtctrl.wallet_cash * 100 / buying_size_value

        actual_zone = self.grid.get_zone_position(self.rtctrl.prices_symbols[symbol])
        zone = self.grid.get_lower_zone_buy_engaged(actual_zone)

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
        if self.df_selling_limits.loc[self.df_selling_limits['symbol'] == symbol, "selling_limits"].iloc[0] < self.limit_sell:
            return True
        else:
            return False

    def set_df_multi(self):
        self.df_selling_limits['symbol'] = self.lst_symbols
        self.df_selling_limits['selling_limits'] = 0

        self.df_grid_multi = pd.DataFrame(columns=['symbol', 'grid', 'previous_zone_position', 'zone_position'])
        self.df_grid_multi['symbol'] = self.lst_symbols
        self.df_grid_multi['previous_zone_position'] = 0
        self.df_grid_multi['zone_position'] = 0

        for symbol in self.lst_symbols:
            self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"] = GridLevelPosition(symbol, params=self.params)

    def get_grid_sell_condition(self, symbol, zone):
        return zone == self.get_lower_zone_buy_engaged(symbol)

    def authorize_merge_current_trades(self):
        return False

    def authorize_merge_buy_long_position(self):
        return False

    def is_open_type_long(self, symbol):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        return self.grid.is_grid_get_open_position()

    # MODIF CEDE DESPERATE SOLUTION
    def is_specific_grid_open_type_long(self, symbol):
        return self.get_open_type(symbol) == self.open_long

    def is_open_type_short_or_long(self, symbol):
        return False

    def grid_exit_range_trend_down(self, symbol):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]

        if self.grid.exit_range_down_trend(self.df_current_data['close'][symbol]):
            return True
        else:
            return False

    def is_out_of_range(self, symbol):
        self.grid = self.df_grid_multi.loc[self.df_grid_multi['symbol'] == symbol, "grid"].iloc[0]
        return self.grid.exit_range(self.df_current_data['close'][symbol])

class GridLevelPosition():
    def __init__(self, symbol, params=None):
        self.UpperPriceLimit = 20500
        self.grid_step = .5 # percent
        self.grid_threshold = 0
        self.df_grid_params = pd.DataFrame()
        if params:
            self.df_grid_params = params.get("grid_df_params", self.df_grid_params)
            config_path = './symbols'
            self.df_grid_params = os.path.join(config_path, self.df_grid_params)
            if isinstance(self.df_grid_params, str):
                self.df_grid_params = pd.read_csv(self.df_grid_params)

        self.UpperPriceLimit = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'UpperPriceLimit'].iloc[0]
        self.LowerPriceLimit = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'LowerPriceLimit'].iloc[0]
        self.OutboundZoneMax = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'OutboundZoneMax'].iloc[0]
        self.OutboundZoneMin = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'OutboundZoneMin'].iloc[0]
        self.grid_step = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'grid_step'].iloc[0]
        self.grid_threshold = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'grid_threshold'].iloc[0]
        self.out_of_grid_range_TP = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'out_of_range_TP'].iloc[0]
        self.out_of_grid_range_SL = self.df_grid_params.loc[self.df_grid_params['symbol'] == symbol, 'out_of_range_SL'].iloc[0]

        if self.OutboundZoneMax < self.UpperPriceLimit \
                or self.OutboundZoneMin > self.LowerPriceLimit:
            print('PARAMETER ERROR: OutboundZone vs PriceLimit inverted')
            tmp1 = self.UpperPriceLimit
            tmp2 = self.OutboundZoneMax
            self.OutboundZoneMax = max(tmp1, tmp2)
            self.UpperPriceLimit = min(tmp1, tmp2)
            tmp1 = self.OutboundZoneMin
            tmp2 = self.LowerPriceLimit
            self.OutboundZoneMin = min(tmp1, tmp2)
            self.LowerPriceLimit = max(tmp1, tmp2)

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
        self.df_grid['flushed'] = False

        self.grid_size = len(self.df_grid)

    def get_out_of_range_TP(self):
        return self.out_of_grid_range_TP

    def get_out_of_range_SL(self):
        return self.out_of_grid_range_SL

    def get_zone_position(self, price):
        try:
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
        if zone_position == -1:
            pass
        else:
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

    def get_LowerPriceLimit(self):
        return self.LowerPriceLimit

    def get_OutboundZoneMax(self):
        return self.OutboundZoneMax

    def get_OutboundZoneMin(self):
        return self.OutboundZoneMin

    def exit_range_down_trend(self, price):
        if price < self.OutboundZoneMin:
            return True
        else:
            return False

    def exit_range_up_trend(self, price):
        if price > self.OutboundZoneMax:
            return True
        else:
            return False

    def exit_range(self, price):
        return self.exit_range_up_trend(price) \
               or self.exit_range_down_trend(price)

    def is_grid_get_open_position(self):
        return self.df_grid['zone_engaged'].any()

    def is_grid_flushed(self):
        return not self.is_grid_get_open_position()
