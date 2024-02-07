import pandas as pd

from src import broker_bitget_api
from src.toolbox import settings_helper

def get_usdt_equity_all_accounts():
    accounts_info = settings_helper.get_accounts_info()
    lst_accounts = []
    lst_usdt_equities = []
    for key, value in accounts_info.items():
        account_id = value.get("id", "")
        my_broker = None
        broker_name = value.get("broker", "")
        if broker_name == "bitget":
            my_broker = broker_bitget_api.BrokerBitGetApi({"account": account_id, "reset_account": False})
            if my_broker:
                usdt_equity = my_broker.get_usdt_equity()

                lst_accounts.append(account_id)
                lst_usdt_equities.append(usdt_equity)

    df = pd.DataFrame({"Accounts": lst_accounts, "USDT_Equity": lst_usdt_equities},
                      index=range(len(lst_accounts)))
    return df
