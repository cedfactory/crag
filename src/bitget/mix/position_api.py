#!/usr/bin/python

from ..client import Client
from ..consts import *


class PositionApi(Client):
    def __init__(self, api_key, api_secret_key, passphrase, use_server_time=False, first=False):
        Client.__init__(self, api_key, api_secret_key, passphrase, use_server_time, first)

    '''
    Obtain the user's single position information
    :return:
    '''
    def single_position(self, symbol, marginCoin):
        params = {}
        if symbol:
            params["symbol"] = symbol
            params["marginCoin"] = marginCoin
            return self._request_with_params(GET, MIX_POSITION_V1_URL + '/singlePosition', params)
        else:
            return "pls check args"

    '''
    Obtain all position information of the user
    productType: Umcbl (USDT professional contract) dmcbl (mixed contract) sumcbl (USDT professional contract simulation disk) sdmcbl (mixed contract simulation disk)
    :return:
    '''
    def all_position(self, productType, marginCoin):
        params = {}
        if productType:
            params["productType"] = productType
            params["marginCoin"] = marginCoin
            return self._request_with_params(GET, MIX_POSITION_V1_URL + '/allPosition', params)
        else:
            return "pls check args"

    def account(self, symbol, marginCoin):
        params = {}
        if symbol:
            params["symbol"] = symbol
            params["marginCoin"] = marginCoin
            return self._request_with_params(GET, MIX_ACCOUNT_V1_URL + '/account', params)
        else:
            return "pls check args"

    def history_position(self, symbol, product_type, start_time, end_time):
        if symbol:
            params = {
                "symbol": symbol,
                'productType': product_type,
                'startTime': start_time,
                'endTime': end_time,
                'pageSize': 100  # Number of records to fetch
            }
            return self._request_with_params(GET, '/api/mix/v1/position/history-position', params)
        else:
            return "pls check args"