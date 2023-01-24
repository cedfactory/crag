from .bitget.mix import market_api as market
from .bitget.mix import account_api as account

from . import broker,rtdp,utils
from dotenv import load_dotenv
import os
import pandas as pd

class BrokerBitGet(broker.Broker):
    def __init__(self, params = None):
        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.trades = []
        self.simulation = False
        self.account = ""
        self.leverage = 0
        self.name = ""
        self.exchange_name = "bitget"
        self.api_key = "BITGET_API_KEY"
        self.api_secret = "BITGET_API_SECRET"
        self.api_password = "BITGET_API_PASSWORD"
        self.chase_limit = False
        if params:
            self.simulation = params.get("simulation", self.simulation)
            if self.simulation == 0 or self.simulation == "0":
                self.simulation = False
            if self.simulation == 1 or self.simulation == "1":
                self.simulation = True
            self.account = params.get("account", self.account)
            self.leverage = params.get("leverage", self.leverage)
            if isinstance(self.leverage, str):
                self.leverage = int(self.leverage)
        if not self.authentification():
            print("[BrokerBitGet] : Problem encountered during authentification")

        self.df_market = self.get_future_market()
         
    def authentification(self):
        authentificated = False
        load_dotenv()
        exchange_api_key = os.getenv(self.api_key)
        exchange_api_secret = os.getenv(self.api_secret)
        exchange_api_password = os.getenv(self.api_password)

        self.marketApi = market.MarketApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)
        self.accountApi = account.AccountApi(exchange_api_key, exchange_api_secret, exchange_api_password, use_server_time=False, first=False)

        #self.df_market = pd.DataFrame()
        return True

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self.authentification():
                print("You must be authenticated to use this method {}".format(fn))
                return None
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    def ready(self):
        return self.marketApi != None and self.accountApi != None

    def log_info(self):
        info = ""
        info += "{}".format(type(self).__name__)
        info += "\nCash : $ {}".format(utils.KeepNDecimals(self.get_cash(), 2))
        info += "\nLeverage : {}".format(self.leverage)
        return info


    @authentication_required
    def get_value(self, symbol):
        return float(self.marketApi.market_price(symbol)["data"]["markPrice"])

    @authentication_required
    def get_commission(self, symbol):
        pass

    @authentication_required
    def execute_trade(self, trade):
        pass

    @authentication_required
    def export_history(self, target):
        pass

    def market_results_to_df(self, markets):
        lst_columns = ['symbol', 'quoteCoin', 'baseCoin', 'symbolType', 'makerFeeRate', 'takerFeeRate', 'minTradeNum']
        df = pd.DataFrame(columns=lst_columns)
        for market in markets['data']:
            lst_info_symbol = [ market['symbol'],
                                market['quoteCoin'],
                                market['baseCoin'],
                                market['symbolType'],
                                float(market['makerFeeRate']),
                                float(market['takerFeeRate']),
                                float(market['minTradeNum'])
                                ]
            df.loc[len(df)] = lst_info_symbol
        return df

    def account_results_to_df(self, markets):
        lst_columns = ['symbol', 'marginCoin', 'available', 'equity', 'usdtEquity', 'locked', 'btcEquity',
                       "size", "actualPrice", 'quoteCoin', 'baseCoin', 'symbolType',
                       'makerFeeRate', 'takerFeeRate', 'minTradeNum']
        df = pd.DataFrame(columns=lst_columns)
        for market in markets['data']:
            if float(market['equity']) > 0 \
                    and float(market['usdtEquity']) > 0 \
                    and float(market['btcEquity']) > 0:
                lst_info_symbol = [ "",
                                    market['marginCoin'],
                                    float(market['available']),
                                    float(market['equity']),
                                    float(market['usdtEquity']),
                                    float(market['locked']),
                                    float(market['btcEquity']),
                                    0, 0, "", "", "", 0, 0, 0]
                df.loc[len(df)] = lst_info_symbol
        return df.copy()

    def get_future_market(self):
        """
        productType
            umcbl USDT perpetual contract
            dmcbl Universal margin perpetual contract
            cmcbl USDC perpetual contract
            sumcbl USDT simulation perpetual contract
            sdmcbl Universal margin simulation perpetual contract
            scmcbl USDC simulation perpetual contract
        """
        dct_market = self.marketApi.contracts('umcbl')
        df_market_umcbl = self.market_results_to_df(dct_market)
        dct_market = self.marketApi.contracts('dmcbl')
        df_market_dmcbl = self.market_results_to_df(dct_market)
        dct_market = self.marketApi.contracts('cmcbl')
        df_market_cmcbl = self.market_results_to_df(dct_market)
        return pd.concat([df_market_umcbl, df_market_dmcbl, df_market_cmcbl])

    def get_df_account(self):
        # update market
        dct_account = self.accountApi.accounts('umcbl')
        df_account_umcbl = self.account_results_to_df(dct_account)
        dct_account = self.accountApi.accounts('dmcbl')
        df_account_dmcbl =  self.account_results_to_df(dct_account)
        dct_account = self.accountApi.accounts('cmcbl')
        df_account_cmcbl = self.account_results_to_df(dct_account)
        self.df_account_assets = pd.concat([df_account_umcbl, df_account_dmcbl, df_account_cmcbl])
        self.df_account_assets.reset_index(inplace=True, drop=True)

    def fill_df_account_from_market(self):
        for idx in self.df_account_assets.index.tolist():
            coin = self.df_account_assets.at[idx, 'marginCoin']
            if coin != "USDT" and coin != "USDC" and coin != "USD":
                self.df_account_assets.at[idx, 'symbol'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "symbol"].values[0]
                self.df_account_assets.at[idx, 'quoteCoin'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "quoteCoin"].values[0]
                self.df_account_assets.at[idx, 'baseCoin'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "baseCoin"].values[0]
                self.df_account_assets.at[idx, 'symbolType'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "symbolType"].values[0]
                self.df_account_assets.at[idx, 'makerFeeRate'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "makerFeeRate"].values[0]
                self.df_account_assets.at[idx, 'takerFeeRate'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "takerFeeRate"].values[0]
                self.df_account_assets.at[idx, 'minTradeNum'] = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == 'USDT'), "minTradeNum"].values[0]
            else:
                self.df_account_assets.at[idx, 'symbol'] = coin
                self.df_account_assets.at[idx, 'quoteCoin'] = coin
                self.df_account_assets.at[idx, 'baseCoin'] = coin
                self.df_account_assets.at[idx, 'size'] = self.df_account_assets.loc[self.df_account_assets["symbol"] == coin, "usdtEquity"].values[0]
                self.df_account_assets.at[idx, 'actualPrice'] = 1

    def fill_price_and_size_from_bitget(self):
        for symbol in self.df_account_assets['symbol'].tolist():
            if self.df_account_assets.loc[self.df_account_assets['symbol'] == symbol, "symbolType"].values[0] == "perpetual":
                self.df_account_assets.loc[self.df_account_assets['symbol'] == symbol, "actualPrice"] = self.get_value(symbol)
                self.df_account_assets.loc[self.df_account_assets['symbol'] == symbol, "size"] = self.df_account_assets.loc[self.df_account_assets['symbol'] == symbol, "available"].values[0]
            else:
                pass

    def print_account_assets(self):
        print(self.df_account_assets)

    def get_list_of_account_assets(self):
        self.get_df_account()
        self.fill_df_account_from_market()
        self.fill_price_and_size_from_bitget()

    def get_symbol(self, coin, base):
        self.get_future_market()
        symbol = self.df_market.loc[(self.df_market['baseCoin'] == coin) & (self.df_market['quoteCoin'] == base), "symbol"].values[0]
        return symbol

    def get_balance(self):
        self.get_list_of_account_assets()
        df_balance = pd.DataFrame(columns=['symbol', 'usdValue'])
        df_balance['symbol'] = self.df_account_assets['symbol']
        df_balance['usdValue'] = self.df_account_assets['usdtEquity']
        df_balance['size'] = self.df_account_assets['available']
        return df_balance

    def get_cash(self, baseCoin='USDT'):
        self.get_list_of_account_assets()
        return self.df_account_assets.loc[self.df_account_assets['symbol'] == baseCoin, "usdtEquity"].values[0]

    def get_usdt_equity(self):
        self.get_list_of_account_assets()
        return self.df_account_assets['usdtEquity'].sum()


