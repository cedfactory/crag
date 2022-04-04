import pytest
import pandas as pd
import os
import json
from src import utils,rtdp_tv

class TestRTDP:
    response_reco = {'elapsed_time': '0:00:02.537734', 'result': {'status': 'ok', 'symbols': '{"symbol":{"0":"WRX\\/USD"},"change1h":{"0":5.8672669763},"rank_change1h":{"0":2},"change24h":{"0":9.9009022334},"rank_change24h":{"0":3},"RECOMMENDATION_30m":{"0":"STRONG_BUY"},"RECOMMENDATION_1h":{"0":"BUY"},"RECOMMENDATION_1m":{"0":"BUY"},"buy_1m":{"0":57},"sell_1m":{"0":11},"neutral_1m":{"0":30},"RECOMMENDATION_15m":{"0":"STRONG_BUY"},"RECOMMENDATION_5m":{"0":"BUY"},"buy_5m":{"0":57},"sell_5m":{"0":15},"neutral_5m":{"0":26}}'}, 'status': 'ok'}
    response_symbol = {'elapsed_time': '0:00:00.394284', 'result': {'WRX_USD': {'info': {'ask': 0.74518, 'askVolume': None, 'average': None, 'baseVolume': None, 'bid': 0.74117, 'bidVolume': None, 'change': 0.07434972029112254, 'close': 0.74221, 'datetime': '2022-04-04T09:00:30.186Z', 'high': None, 'info': {'ask': '0.74518', 'baseCurrency': 'WRX', 'bid': '0.74117', 'change1h': '0.05749009774029008', 'change24h': '0.10017342839778842', 'changeBod': '0.07619696662123365', 'enabled': True, 'highLeverageFeeExempt': True, 'largeOrderThreshold': '500.0', 'last': '0.74221', 'minProvideSize': '1.0', 'name': 'WRX/USD', 'postOnly': False, 'price': '0.74221', 'priceHigh24h': '0.7484', 'priceIncrement': '0.00001', 'priceLow24h': '0.67203', 'quoteCurrency': 'USD', 'quoteVolume24h': '36456.28691', 'restricted': False, 'sizeIncrement': '1.0', 'type': 'spot', 'underlying': None, 'volumeUsd24h': '36456.28691'}, 'last': 0.74221, 'low': None, 'open': 0.6678602797088774, 'percentage': 10.017342839778841, 'previousClose': None, 'quoteVolume': 36456.28691, 'symbol': 'WRX/USD', 'timestamp': 1649062830186, 'vwap': None}, 'status': 'ok'}}, 'status': 'ok'}

    def test_rtdp_tv_next(self, mocker):
        
        # context
        my_rtdp_tv = rtdp_tv.RTDPTradingView()
        mocker.patch('src.utils.fdp_request', side_effect=[self.response_reco, self.response_symbol])

        # action
        data_from_next = my_rtdp_tv.next()
        df_portfolio_from_next = pd.read_json(data_from_next["portfolio"])
        selection_from_next = df_portfolio_from_next['symbol'].to_list()

        data_from_get_current_data = my_rtdp_tv.get_current_data()
        df_portfolio_from_get_current_data = pd.read_json(data_from_get_current_data["portfolio"])
        selection_from_get_current_data = df_portfolio_from_get_current_data['symbol'].to_list()

        # expectation
        expected_selection = ['WRX/USD']
        assert(selection_from_next == expected_selection)
        assert(selection_from_get_current_data == expected_selection)

    def test_rtdp_tv_record(self, mocker):
        
        # context
        my_rtdp_tv = rtdp_tv.RTDPTradingView()
        mocker.patch('src.utils.fdp_request', side_effect=[self.response_reco, self.response_symbol])

        # action 1 : record history
        filename = "test_history.csv"
        my_rtdp_tv.record(1, 1, filename)

        # action 2 : read history
        params = {'infile':filename}
        my_rtdp_tv2 = rtdp_tv.RTDPTradingView(params)

        data = my_rtdp_tv2.next()
        df_data = pd.read_json(data["portfolio"])
        selection = df_data['symbol'].to_list()
        expected_selection = ['WRX/USD']
        assert(selection == expected_selection)

        # cleaning
        os.remove(filename)
