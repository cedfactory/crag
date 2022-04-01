import pytest
import pandas as pd
import os
import json
from src import rtdp_tv

class TestRTDP:
    response = '{"portfolio": "{\\\"symbol\\\":{\\\"0\\\":\\\"GOG\\/USD\\\",\\\"1\\\":\\\"SNX\\/USD\\\"},\\\"change1h\\\":{\\\"0\\\":21.8831410495,\\\"1\\\":7.3734667983},\\\"rank_change1h\\\":{\\\"0\\\":0,\\\"1\\\":1},\\\"change24h\\\":{\\\"0\\\":47.855530474,\\\"1\\\":9.4882116159},\\\"rank_change24h\\\":{\\\"0\\\":1,\\\"1\\\":17},\\\"RECOMMENDATION_30m\\\":{\\\"0\\\":\\\"STRONG_BUY\\\",\\\"1\\\":\\\"STRONG_BUY\\\"},\\\"RECOMMENDATION_15m\\\":{\\\"0\\\":\\\"STRONG_BUY\\\",\\\"1\\\":\\\"STRONG_BUY\\\"},\\\"RECOMMENDATION_1m\\\":{\\\"0\\\":\\\"BUY\\\",\\\"1\\\":\\\"BUY\\\"},\\\"buy_1m\\\":{\\\"0\\\":57,\\\"1\\\":53},\\\"sell_1m\\\":{\\\"0\\\":15,\\\"1\\\":11},\\\"neutral_1m\\\":{\\\"0\\\":26,\\\"1\\\":34},\\\"RECOMMENDATION_1h\\\":{\\\"0\\\":\\\"STRONG_BUY\\\",\\\"1\\\":\\\"STRONG_BUY\\\"},\\\"RECOMMENDATION_2h\\\":{\\\"0\\\":\\\"STRONG_BUY\\\",\\\"1\\\":\\\"STRONG_BUY\\\"},\\\"buy_2h\\\":{\\\"0\\\":69,\\\"1\\\":61},\\\"sell_2h\\\":{\\\"0\\\":0,\\\"1\\\":0},\\\"neutral_2h\\\":{\\\"0\\\":30,\\\"1\\\":38},\\\"RECOMMENDATION_5m\\\":{\\\"0\\\":\\\"STRONG_BUY\\\",\\\"1\\\":\\\"STRONG_BUY\\\"},\\\"buy_5m\\\":{\\\"0\\\":65,\\\"1\\\":61},\\\"sell_5m\\\":{\\\"0\\\":7,\\\"1\\\":3},\\\"neutral_5m\\\":{\\\"0\\\":26,\\\"1\\\":34},\\\"RECOMMENDATION_4h\\\":{\\\"0\\\":\\\"STRONG_BUY\\\",\\\"1\\\":\\\"STRONG_BUY\\\"},\\\"buy_4h\\\":{\\\"0\\\":65,\\\"1\\\":65},\\\"sell_4h\\\":{\\\"0\\\":0,\\\"1\\\":0},\\\"neutral_4h\\\":{\\\"0\\\":34,\\\"1\\\":34}}", "symbols": {"GOG_USD": {"info": {"ask": 0.82, "askVolume": null, "average": null, "baseVolume": null, "bid": 0.81825, "bidVolume": null, "change": 0.39127087093862817, "close": 0.81875, "datetime": "2022-04-01T17:14:44.816Z", "high": null, "info": {"ask": "0.82", "baseCurrency": "GOG", "bid": "0.81825", "change1h": "0.21883141049497581", "change24h": "0.4778880866425993", "changeBod": "0.365721434528774", "enabled": true, "highLeverageFeeExempt": true, "largeOrderThreshold": "500.0", "last": "0.81875", "minProvideSize": "1.0", "name": "GOG/USD", "postOnly": false, "price": "0.81875", "priceHigh24h": "0.82125", "priceIncrement": "0.00025", "priceLow24h": "0.55325", "quoteCurrency": "USD", "quoteVolume24h": "185615.90775", "restricted": false, "sizeIncrement": "1.0", "type": "spot", "underlying": null, "volumeUsd24h": "185615.90775"}, "last": 0.81875, "low": null, "open": 0.4274791290613718, "percentage": 47.78880866425993, "previousClose": null, "quoteVolume": 185615.90775, "symbol": "GOG/USD", "timestamp": 1648833284816, "vwap": null}, "status": "ok"}, "SNX_USD": {"info": {"ask": 7.609, "askVolume": null, "average": null, "baseVolume": null, "bid": 7.593, "bidVolume": null, "change": 0.7083657274295572, "close": 7.604, "datetime": "2022-04-01T17:14:45.204Z", "high": null, "info": {"ask": "7.609", "baseCurrency": "SNX", "bid": "7.593", "change1h": "0.07211843496651392", "change24h": "0.09315698677400805", "changeBod": "0.10893976957853288", "enabled": true, "highLeverageFeeExempt": true, "largeOrderThreshold": "500.0", "last": "7.604", "minProvideSize": "0.1", "name": "SNX/USD", "postOnly": false, "price": "7.604", "priceHigh24h": "7.6845", "priceIncrement": "0.0005", "priceLow24h": "6.636", "quoteCurrency": "USD", "quoteVolume24h": "5174560.9909", "restricted": false, "sizeIncrement": "0.1", "type": "spot", "underlying": null, "volumeUsd24h": "5174560.9909"}, "last": 7.604, "low": null, "open": 6.895634272570443, "percentage": 9.315698677400805, "previousClose": null, "quoteVolume": 5174560.9909, "symbol": "SNX/USD", "timestamp": 1648833285204, "vwap": null}, "status": "ok"}}}'

    def test_rtdp_tv_next(self, mocker):
        
        # context
        mock_rtdp_tv = rtdp_tv.RTDPTradingView()
        mocker.patch.object(mock_rtdp_tv, "_fetch_data", return_value=json.loads(self.response))

        # action
        data_from_next = mock_rtdp_tv.next()
        df_portfolio_from_next = pd.read_json(data_from_next["portfolio"])
        selection_from_next = df_portfolio_from_next['symbol'].to_list()

        data_from_get_current_data = mock_rtdp_tv.get_current_data()
        df_portfolio_from_get_current_data = pd.read_json(data_from_get_current_data["portfolio"])
        selection_from_get_current_data = df_portfolio_from_get_current_data['symbol'].to_list()

        # expectation
        expected_selection = ['GOG/USD', 'SNX/USD']
        assert(selection_from_next == expected_selection)
        assert(selection_from_get_current_data == expected_selection)

    def test_rtdp_tv_record(self, mocker):
        
        # context
        mock_rtdp_tv = rtdp_tv.RTDPTradingView()
        mocker.patch.object(mock_rtdp_tv, "_fetch_data", return_value=json.loads(self.response))

        # action 1 : record history
        filename = "test_history.csv"
        mock_rtdp_tv.record(1, 1, filename)

        # action 2 : read history
        params = {'infile':filename}
        rtdp_tv2 = rtdp_tv.RTDPTradingView(params)

        data = rtdp_tv2.next()
        df_data = pd.read_json(data["portfolio"])
        selection = df_data['symbol'].to_list()
        expected_selection = ['GOG/USD', 'SNX/USD']
        assert(selection == expected_selection)

        # cleaning
        os.remove(filename)
