import pandas as pd
import time
import threading

class WS_Account_Data:
    def __init__(self, df_open_positions=None, df_open_orders=None, df_triggers=None, dct_account=None,
                 dct_prices=None):
        """
        Initialize the ws_data object with dataframes and dictionary.

        Parameters:
            df_open_positions (pd.DataFrame): DataFrame for open positions. Defaults to empty DataFrame.
            df_open_orders (pd.DataFrame): DataFrame for open orders. Defaults to empty DataFrame.
            df_triggers (pd.DataFrame): DataFrame for triggers. Defaults to empty DataFrame.
            dct_account (dict): Dictionary for account info. Defaults to empty dict.
            dct_prices (pd.DataFrame): DataFrame for prices (despite the naming, this is a DataFrame). Defaults to empty DataFrame.
        """
        columns_triggers = [
            "timestamp",
            'planType', 'symbol', 'size', 'side', 'orderId', 'orderType',
            'clientOid', 'price', 'triggerPrice', 'triggerType', 'marginMode',
            'gridId', 'strategyId', 'trend', 'executeOrderId', 'planStatus'
        ]

        columns_open_orders = [
            "timestamp",
            "symbol", "size", "orderId", "clientOid", "notional", "orderType", "force", "side",
            "fillPrice", "tradeId", "baseVolume", "accBaseVolume", "fillTime", "priceAvg", "status",
            "cTime", "uTime", "stpMode", "feeDetail", "enterPointSource", "tradeSide", "orderSource",
            "leverage"
        ]
        columns_open_positions = [
            "timestamp",
            "symbol", "holdSide", "leverage", "marginCoin",
            "available", "total", "usdtEquity",
            "marketPrice", "averageOpenPrice",
            "achievedProfits", "unrealizedPL", "liquidationPrice"]
        dct_account = {
            "available": None,
            "maxOpenPosAvailable": None,
            "usdtEquity": None
        }
        columns_prices = [
            "timestamp", "symbols", "values"
        ]
        current_timestamp = time.time()

        # Fall back to empty DataFrames if any of the provided DFs is None
        self._df_open_positions = (
            df_open_positions
            if df_open_positions is not None
            else pd.DataFrame(columns=columns_open_positions)
        )
        self._df_open_orders = (
            df_open_orders
            if df_open_orders is not None
            else pd.DataFrame(columns=columns_open_orders)
        )
        self._df_triggers = (
            df_triggers
            if df_triggers is not None
            else pd.DataFrame(columns=columns_triggers)
        )

        # Update timestamp for each non-empty DataFrame
        for df in (self._df_open_positions, self._df_open_orders, self._df_triggers):
            if not df.empty:
                df["timestamp"] = current_timestamp
            elif 'timestamp' not in df.columns:
                df['timestamp'] = pd.Series(dtype='datetime64[ns]')
        self._dct_account = dct_account if dct_account is not None else dct_account
        self._df_prices = dct_prices if dct_prices is not None else pd.DataFrame(columns=columns_prices)

        self.verbose = False

        self.ws_triggers = self.ws_open_orders = self.ws_open_positions = self.ws_account = self.ws_prices = False

    def set_ws_prices(self, dct_ticker_prices):
        self.ws_prices = True
        if dct_ticker_prices:
            self._df_prices = pd.DataFrame.from_dict(dct_ticker_prices,
                                                     orient='index').reset_index(drop=True)
            self._df_prices['timestamp'] = pd.to_numeric(self._df_prices['timestamp'], errors='coerce')
            self._df_prices['values'] = pd.to_numeric(self._df_prices['values'], errors='coerce')
        if self.verbose:
            print("\nself.df_prices:")
            print(self._df_prices.to_string())
            print("\n")

    def set_ws_account(self, dct_account):
        self.ws_account = True
        self._dct_account = dct_account
        if self.verbose:
            print("\nself.dct_account:")
            print(self._dct_account)
            print("\n")

    def set_ws_open_positions(self, df_open_positions):
        self.ws_open_positions = True
        if not df_open_positions.empty:
            # 1) Add current timestamp to the incoming rows
            current_timestamp = time.time()
            df_open_positions["timestamp"] = current_timestamp

            # 2) Concatenate with the existing DataFrame
            self._df_open_positions = pd.concat(
                [self._df_open_positions, df_open_positions],
                ignore_index=True
            )

            # 3) Keep only the row with the most recent timestamp for each posId
            self._df_open_positions = self._df_open_positions.loc[
                self._df_open_positions.groupby("posId")["timestamp"].idxmax()
            ]

        if self.verbose:
            print("\nself.df_open_positions:")
            print(self._df_open_positions.to_string())
            print("\n")

    def set_ws_open_orders(self, df_open_orders):
        self.ws_open_orders = True
        if not df_open_orders.empty:
            # 1) Add current timestamp to the incoming rows
            current_timestamp = time.time()
            df_open_orders["timestamp"] = current_timestamp

            # 2) Concatenate with the existing DataFrame
            self._df_open_orders = pd.concat(
                [self._df_open_orders, df_open_orders],
                ignore_index=True
            )

            # 3) Keep only the row with the most recent timestamp for each posId
            self._df_open_orders = self._df_open_orders.loc[
                self._df_open_orders.groupby("orderId")["timestamp"].idxmax()
            ]

        if self.verbose:
            print("\nself._df_open_orders:")
            print(self._df_open_orders.to_string())
            print("\n")

    def set_ws_triggers(self, df_triggers):
        self.ws_triggers = True
        if not df_triggers.empty:
            # 1) Add current timestamp to the incoming rows
            current_timestamp = time.time()
            df_triggers["timestamp"] = current_timestamp

            # 2) Concatenate with the existing DataFrame
            self._df_triggers = pd.concat(
                [self._df_triggers,
                 df_triggers],
                ignore_index=True
            )

            self._df_triggers["timestamp"] = pd.to_numeric(self._df_triggers["timestamp"], errors='coerce')

            # 3) Keep only the row with the most recent timestamp for each posId
            self._df_triggers = self._df_triggers.loc[
                self._df_triggers.groupby("orderId")["timestamp"].idxmax()
            ]

        if self.verbose:
            print("\nself._df_triggers:")
            print(self._df_triggers.to_string())
            print("\n")

    # Accessor functions (getters)
    def get_ws_open_positions(self):
        """Return the open positions DataFrame."""
        return {"type": "OPEN_POSITIONS",
                "data": self._df_open_positions.drop("timestamp", axis=1).to_dict()}

    def get_ws_open_orders(self):
        """Return the open orders DataFrame."""
        return {"type": "OPEN_ORDERS", "data": self._df_open_orders.drop("timestamp", axis=1).to_dict()}

    def get_ws_triggers(self):
        """Return the triggers DataFrame."""
        return {"type": "TRIGGERS",
                "data": self._df_triggers.drop("timestamp", axis=1).to_dict()}

    def get_ws_account(self):
        """Return the account dictionary."""
        return {"type": "ACCOUNT",
                    "data": self._dct_account}

    def get_ws_prices(self):
        """Return the prices DataFrame."""
        self._df_prices["symbols"] = self._df_prices["symbols"].str.replace("USDT", "", regex=False)
        return {"type": "PRICES",
                "data": self._df_prices.to_dict()}

    def get_ws_df_prices(self):
        """Return the prices DataFrame."""
        return self._df_prices

    def get_usdt_equity_available(self):
        return {
            "type": "USDT_EQUITY_AVAILABLE",
            "usdtEquity": self._dct_account["usdtEquity"],
            "available": self._dct_account["available"]
        }

    def get_value(self, symbol):
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        matching_rows = self._df_prices[self._df_prices['symbols'] == symbol.replace('_UMCBL', "")]

        # Check if any rows were found; if not, return None.
        if matching_rows.empty:
            return None

        # Return the first matching value from the 'values' column.
        return {
            "type": "PRICE",
            "symbol": symbol,
            "value": float(matching_rows['values'].iloc[0])
        }

    def get_values(self, symbols):
        df_prices = self._df_prices.copy()
        df_prices['symbols'] = df_prices['symbols'] + "_UMCBL"
        available_symbols = set(df_prices['symbols'])
        # Check for any missing symbols.
        missing_symbols = set(symbols) - available_symbols
        if missing_symbols:
            # Return None if any requested symbol is not found in the DataFrame.
            return None

        # Return only rows with symbols that are in the provided list.
        return {
            "type": "PRICE",
            "value": df_prices[df_prices['symbols'].isin(symbols)]
        }

    def get_data_status(self):
        return self.ws_triggers \
               and self.ws_open_orders \
               and self.ws_open_positions \
               and self.ws_account \
               and self.ws_prices

# DATA UTILS
# CEDE TO BE MOVED TO UTILS OR NOT...
def convert_triggers_push_to_response(item):
    """
    Convert a single websocket push dictionary to the desired format.
    """
    # Mapping websocket planType values to REST API planType values
    ws_plan_type = item.get("planType")
    plan_type_mapping = {
        "pl": "normal_plan",  # default trigger order
        "tp": "profit_plan",  # partial take profit
        "sl": "loss_plan",  # partial stop loss
        "ptp": "pos_profit",  # position take profit
        "psl": "pos_loss",  # position stop loss
        "track": "track_plan",  # trailing stop order
        "mtpsl": "moving_plan"  # trailing TP/SL
    }

    converted = {
        "planType": plan_type_mapping.get(ws_plan_type, ws_plan_type),  # Convert planType
        "symbol": item.get("instId"),  # Mapping instId -> symbol
        "size": float(item.get("size")),
        "side": item.get("side"),
        "orderId": item.get("orderId"),
        "orderType": item.get("orderType"),
        "clientOid": item.get("clientOid"),
        "price": item.get("price"),
        "triggerPrice": float(item.get("triggerPrice")),
        "triggerType": item.get("triggerType"),
        "marginMode": "",  # May require custom logic or default value
        "executeOrderId": "",
        'gridId': "",
        'strategyId': "",
        'trend': "",
        "planStatus": item.get("status"),  # Mapping status -> planStatus
    }
    return converted

def convert_triggers_convert_df_to_df(df):
    """
    Convert a DataFrame of push parameter dictionaries (websocket data) to a pandas DataFrame
    using the conversion function. If df is None or empty, return an empty DataFrame with the expected columns.
    """
    # Define the expected columns (order matters)
    """
    columns = [
        "planType", "symbol", "size", "orderId", "clientOid", "price", "executePrice",
        "callbackRatio", "triggerPrice", "triggerType", "planStatus", "side", "posSide",
        "marginCoin", "marginMode", "enterPointSource", "tradeSide", "posMode",
        "orderType", "orderSource", "cTime", "uTime",
        "stopSurplusExecutePrice", "stopSurplusTriggerPrice", "stopSurplusTriggerType",
        "stopLossExecutePrice", "stopLossTriggerPrice", "stopLossTriggerType"
    ]
    """
    columns = [
        'planType', 'symbol', 'size', 'side', 'orderId', 'orderType',
        'clientOid', 'price', 'triggerPrice', 'triggerType', 'marginMode',
        'gridId', 'strategyId', 'trend', 'executeOrderId', 'planStatus'
    ]

    # If the DataFrame is None or empty, return an empty DataFrame with the defined columns
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    # Convert DataFrame to a list of dictionaries (one for each row)
    rows = df.to_dict('records')
    # Convert each dictionary using the conversion function
    converted_list = [convert_triggers_push_to_response(item) for item in rows]
    # Return the DataFrame with the specified column order
    return pd.DataFrame(converted_list, columns=columns)

def get_v1_side_and_trade_side(push_order: dict) -> dict:
    # V2 fields
    side_v2 = push_order.get("side", "").lower()  # Expected: "buy" or "sell"
    pos_side_v2 = push_order.get("posSide", "").lower()  # Expected: "long" or "short"
    pos_mode_v2 = push_order.get("posMode", "")  # Expected: "one_way_mode" or "hedge-mode"

    # Determine hold mode based on V2 posMode
    if pos_mode_v2 == "one_way_mode":
        hold_mode = "single_hold"
    elif pos_mode_v2 == "hedge-mode":
        hold_mode = "double_hold"
    else:
        hold_mode = ""

    # Initialize variables
    v1_side = ""
    v1_trade_side = ""

    # Map V2 fields to V1 based on hold mode
    if hold_mode == "single_hold":
        # In single_hold mode, we use a different naming for side and ignore tradeSide.
        if side_v2 == "buy":
            v1_side = "buy_single"
        elif side_v2 == "sell":
            v1_side = "sell_single"
        # tradeSide remains empty for single_hold mode.
    elif hold_mode == "double_hold":
        # In double_hold mode, determine open/close based on the combination of side and posSide.
        if side_v2 == "buy":
            if pos_side_v2 == "long":
                v1_side = "open_long"
                v1_trade_side = "open_long"
            elif pos_side_v2 == "short":
                v1_side = "close_short"
                v1_trade_side = "close_short"
        elif side_v2 == "sell":
            if pos_side_v2 == "short":
                v1_side = "open_short"
                v1_trade_side = "open_short"
            elif pos_side_v2 == "long":
                v1_side = "close_long"
                v1_trade_side = "close_long"

    return {"side": v1_side, "tradeSide": v1_trade_side}

# Order ws: https://www.bitget.com/api-doc/contract/websocket/private/Order-Channel
# Order API REST: https://bitgetlimited.github.io/apidoc/en/mix/#get-open-order
def convert_open_order_push_to_response(push_order: dict) -> dict:
    reduce_only = push_order.get("reduceOnly", "")
    mapping_reduce_only = {
        "yes": "True",
        "no": "False"
    }

    side_v2 = push_order.get("side", "")            # buy / sell
    pos_side_v2 = push_order.get("posSide", "")     # long / short
    posMode_v2 = push_order.get("posMode", "")

    push_order_example = {
        "side": side_v2,
        "posSide": pos_side_v2,
        "posMode": "hedge-mode"  # double hold mode
    }
    result_side = get_v1_side_and_trade_side(push_order_example)

    hold_mode_mapping = {
        "one_way_mode": "single_hold",
        "hedge-mode": "double_hold"
    }
    holdMode = hold_mode_mapping.get(posMode_v2, "")

    return {
            "symbol": push_order.get("instId", ""),
            "price": float(push_order.get("price", "")),
            "side": result_side.get("side", ""),
            "size": float(push_order.get("size", "")),
            "leverage": float(push_order.get("leverage", "")),
            "marginCoin": push_order.get("marginCoin", ""),
            "marginMode": push_order.get("marginMode", ""),
            "clientOid": push_order.get("clientOid", ""),
            "orderId": push_order.get("orderId", ""),

            "state": push_order.get("status", ""),
            "fee": float(push_order.get("feeDetail", [{}])[0].get("fee")),
        }

def convert_open_orders_push_list_to_df(df):
    """
    Convert a list of push order dictionaries into a pandas DataFrame
    with columns matching the GET /api/mix/v1/order/current response structure.

    If lst is None, return an empty DataFrame with the same columns.

    ws: https://www.bitget.com/api-doc/contract/websocket/private/Order-Channel
    API REST: GET /api/mix/v1/order/current     https://bitgetlimited.github.io/apidoc/en/mix/#get-open-order
    """
    columns =[
        "symbol",
        "price",
        "side",
        "size",
        "leverage",
        "marginCoin",
        "marginMode",
        "clientOid",
        "orderId",

        "state",
        "fee",
    ]

    if df is None:
        return pd.DataFrame(columns=columns)

    # Convert DataFrame to a list of dictionaries (one for each row)
    rows = df.to_dict('records')
    # Convert each dictionary using the conversion function
    converted_list = [convert_open_order_push_to_response(item) for item in rows]
    return pd.DataFrame(converted_list, columns=columns)
