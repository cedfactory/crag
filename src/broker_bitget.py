from . import broker, rtdp, utils
import pandas as pd
import re
import json
from . bitget import exceptions
from concurrent.futures import wait, ALL_COMPLETED, ThreadPoolExecutor
import threading

class BrokerBitGet(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)
        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.name = ""
        self.exchange_name = "bitget"
        self.chase_limit = False
        self.log_trade = ""
        self.zero_print = True
        self.trade_symbol = ""
        self.clientOid = ""
        if params:
            self.zero_print = params.get("zero_print", self.zero_print)
            if isinstance(self.zero_print, str):
                self.zero_print = self.zero_print == "True" # convert to boolean

        #if not self._authentification():
        #    print("[BrokerBitGet] : Problem encountered during authentification")

        self.clientOIdprovider = utils.ClientOIdProvider()

        self.df_grid_id_match = pd.DataFrame(columns=["orderId", "grid_id", "strategy_id", "trend"])

        self.lock_df_grid_id_match = threading.Lock()
        self.lock_place_trigger_order_v2 = threading.Lock()

        self.execute_timer = None
        self.iter_execute_trades = 0
        self.iter_set_open_orders_gridId = 0

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('lock_df_grid_id_match', None)
        state.pop('lock_place_trigger_order_v2', None)

        if 'lock_df_grid_id_match' not in state:
            print("lock_df_grid_id_match has been removed from the state.")
        else:
            print("ERROR lock_df_grid_id_match still in state")

        if 'lock_place_trigger_order_v2' not in state:
            print("lock_place_trigger_order_v2 has been removed from the state.")
        else:
            print("ERROR lock_place_trigger_order_v2 still in state")

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.lock_df_grid_id_match = threading.Lock()
        self.lock_place_trigger_order_v2 = threading.Lock()

    def authentication_required(fn):
        """decoration for methods that require authentification"""
        def wrapped(self, *args, **kwargs):
            if not self._authentification():
                self.log("You must be authenticated to use this method {}".format(fn))
                return None
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    def ready(self):
        return self.marketApi != None and self.accountApi != None

    def log_info(self):
        info = ""
        info += "{}".format(type(self).__name__)
        cash, _, _ = self.get_available_cash()
        info += "\nCash : $ {}".format(utils.KeepNDecimals(cash, 2))
        return info

    def log_info_trade(self):
        return self.log_trade

    def clear_log_info_trade(self):
        del self.log_trade
        self.log_trade = ""

    @authentication_required
    def get_commission(self, symbol):
        pass

    @authentication_required
    def get_minimum_size(self, symbol):
        pass

    @authentication_required
    def set_symbol_leverage(self, symbol, leverage):
        pass

    @authentication_required
    def set_symbol_margin(self, symbol, margin):
        pass

    def set_execute_time_recorder(self, execute_timer):
        if self.execute_timer is not None:
            del self.execute_timer
        self.execute_timer = execute_timer

        self.iter_execute_trades = 0
        self.iter_set_open_orders_gridId = 0

    @authentication_required
    def set_margin_mode_and_leverages(self):
        for index, row in self.df_symbols.iterrows():
            symbol = row["symbol"]
            if "margin_mode" in row:
                self.set_symbol_margin(symbol, row["margin_mode"])
                self.set_symbol_margin(symbol, row["margin_mode"])
            if "leverage_long" in row:
                if row["leverage_long"] != 0:
                    self.set_symbol_leverage(symbol, row["leverage_long"], "long")
                else:
                    cross_margin_leverage, long_leverage, short_leverage = self.get_account_symbol_leverage(symbol)
                    self.df_symbols.at[index, "leverage_long"] = long_leverage
            if "leverage_short" in row:
                if row["leverage_short"] != 0:
                    self.set_symbol_leverage(symbol, row["leverage_short"], "short")
                else:
                    cross_margin_leverage, long_leverage, short_leverage = self.get_account_symbol_leverage(symbol)
                    self.df_symbols.at[index, "leverage_short"] = short_leverage

    def normalize_grid_df_buying_size_size(self, df_buying_size):
        if not isinstance(df_buying_size, pd.DataFrame) \
                or len(df_buying_size) == 0:
            return

        for index, row in df_buying_size.iterrows():
            symbol = row['symbol']
            size = row['buyingSize']
            normalized_size = self.normalize_size(symbol, size)
            df_buying_size.at[index, 'buyingSize'] = normalized_size
        return df_buying_size

    def check_validity_order(self, order):
        # CEDE avoid empty dict field: from order -> trade
        lst_key = ["time", "success", "orderId", "clientOid", "tradeId", "symbol_price", "gross_price",
                   "buying_fee", "net_size", "net_price", "bought_gross_price", "buying_price"]
        for key in lst_key:
            if key not in order:
                order[key] = None
        return order

    def store_gridId_orderId(self, trade):
        orderId = trade.orderId
        gridId = trade.grid_id
        strategyId = trade.strategy_id
        if trade.success and orderId != None and gridId != -1:
            self.add_gridId_orderId(gridId, orderId, strategyId, None)
        del orderId
        del gridId

    def add_gridId_orderId(self, gridId, orderId, strategyId, trend=None):
        with self.lock_df_grid_id_match:
            try:
                self.df_grid_id_match.loc[len(self.df_grid_id_match)] = [orderId, gridId, strategyId, trend]
                self.df_grid_id_match = self.df_grid_id_match.reset_index(drop=True)
            except:
                exit(555) # DEBUG

    def clear_gridId_orderId(self, lst_orderId):
        if False \
                and (len(self.df_grid_id_match) > len(lst_orderId)):
            self.df_grid_id_match = self.df_grid_id_match[self.df_grid_id_match['orderId'].isin(lst_orderId)]

    def check_orderId_record_status(self, orderId):
        return orderId in self.df_grid_id_match["orderId"].to_list()

    class OrderToTradeConverter:
        def __init__(self, order):
            for key, value in order.items():
                setattr(self, key, value)

        def set_all_to_none(self):
            for attr_name in vars(self):  # Get all attributes of the instance
                setattr(self, attr_name, None)  # Set each attribute to None

    def set_init_memory_usage(self, memory_usage):
        self.init_memory_usage = memory_usage

    @authentication_required
    def execute_trigger(self, trigger):
        symbol = self.trade_symbol = self._get_symbol(trigger["symbol"])
        order_side = trigger["type"]
        if not isinstance(order_side, str):
            order_side = str(order_side)
        if "LONG" in order_side:
            leverage = self.get_leverage_long(symbol)
        elif "SHORT" in order_side:
            leverage = self.get_leverage_short(symbol)
        sizeMultiplier = self.get_sizeMultiplier(symbol)
        amount = utils.normalize_size(trigger["gross_size"] * leverage, sizeMultiplier)
        amount = str(amount)
        trigger_price = trigger.get("trigger_price", None)
        if not isinstance(trigger_price, str):
            trigger_price = str(trigger_price)
        range_rate = trigger.get("range_rate", None)
        if not isinstance(range_rate, str):
            range_rate = str(range_rate)
        order_type = trigger.get("order_type", None)
        if not isinstance(order_type, str):
            order_type = str(order_type)
        trigger_type = trigger.get("triggerType", None)
        if not isinstance(trigger_type, str):
            trigger_type = str(trigger_type)
        grid_id = str(trigger.get("grid_id", ""))
        if not isinstance(grid_id, str):
            grid_id = str(grid_id)
        trend = str(trigger.get("trend", ""))
        tp = str(trigger.get("TP", ""))
        sl = str(trigger.get("SL", ""))
        stopSurplusTriggerType = ""
        stopLossTriggerType = ""
        if sl != "":
            stopLossTriggerType = "mark_price"
        if tp != "":
            stopSurplusTriggerType = "mark_price"

        side = trigger["type"]
        if "grid_id" in trigger:
            side = side + "__" + str(trigger["grid_id"]) + "__"
        if "strategy_id" in trigger:
            side = side + "--" + str(trigger["strategy_id"]) + "--"
        clientOid = self.clientOIdprovider.get_name(symbol, side)

        if "OPEN" in order_side and "LONG" in order_side:
            trade_side = 'Open'
            side = 'Buy'
        elif "OPEN" in order_side and "SHORT" in order_side:
            trade_side = 'Open'
            side = 'Sell'
        elif "CLOSE" in order_side and "LONG" in order_side:
            trade_side = 'Close'
            side = 'Buy'
        elif "CLOSE" in order_side and "SHORT" in order_side:
            trade_side = 'Close'
            side = 'Sell'

        if trigger_price and range_rate and amount and order_side and amount and side and trade_side:
            msg = "!!!!! EXECUTE TRIGGER ORDER !!!!!" + "\n"
            trigger_price = str(self.normalize_price(symbol, float(trigger_price)))
            msg += "{} size: {} trigger_price: {}".format(order_side, amount, trigger_price) + "\n"

            with self.lock_place_trigger_order_v2:
                transaction = self._place_trigger_order_v2(symbol,  planType="normal_plan", triggerPrice=trigger_price,
                                                           marginCoin="USDT", size=amount, side=side,
                                                           tradeSide=trade_side, reduceOnly="YES",
                                                           orderType="market",
                                                           triggerType="mark_price",
                                                           clientOid=clientOid,
                                                           callbackRatio="",
                                                           price="",
                                                           sl=sl,
                                                           tp=tp,
                                                           stopLossTriggerType=stopLossTriggerType,
                                                           stopSurplusTriggerType=stopSurplusTriggerType
                                                           )

        if "msg" in transaction and transaction["msg"] == "success" and "data" in transaction:
            orderId = transaction["data"]["orderId"]
            if "strategy_id" in trigger:
                strategy_id = trigger["strategy_id"]
                self.add_gridId_orderId(grid_id, orderId, strategy_id, trend)
            transaction_failure = False
        else:
            transaction_failure = True
            msg += "TRADE FAILED: {} size: {} trigger_price: {}".format(order_side, amount, trigger_price) + "\n"
        self.log_trade = self.log_trade + msg.upper()

        if not transaction_failure:
            trigger["trade_status"] = "SUCCESS"
            trigger["orderId"] = orderId
        elif transaction_failure:
            trigger["trade_status"] = "FAILED"
            trigger["orderId"] = None
        else:
            trigger["trade_status"] = "UNKNOWN"
        del msg

        return trigger

    @authentication_required
    def execute_sltp_trailling(self, order):
        symbol = self.trade_symbol = self._get_symbol(order["symbol"])
        order_side = order["type"]
        if not isinstance(order_side, str):
            order_side = str(order_side)
        if "LONG" in order_side:
            leverage = self.get_leverage_long(symbol)
        elif "SHORT" in order_side:
            leverage = self.get_leverage_short(symbol)
        sizeMultiplier = self.get_sizeMultiplier(symbol)
        amount = utils.normalize_size(order["gross_size"] * leverage, sizeMultiplier)
        amount = str(amount)
        trigger_price = order.get("trigger_price", None)
        if not isinstance(trigger_price, str):
            trigger_price = str(trigger_price)
            initial_trigger_price = str(trigger_price)
        range_rate = order.get("range_rate", None)
        if not isinstance(range_rate, str):
            range_rate = str(range_rate)
        order_type = order.get("order_type", None)
        if not isinstance(order_type, str):
            order_type = str(order_type)
        trigger_type = order.get("triggerType", None)
        if not isinstance(trigger_type, str):
            trigger_type = str(trigger_type)
        grid_id = str(order.get("grid_id", ""))
        if not isinstance(grid_id, str):
            grid_id = str(grid_id)
        strategy_id = str(order["strategy_id"])
        trend = str(order["trend"])

        clientOid = self.clientOIdprovider.get_name(symbol, order["type"])

        if "OPEN" in order_side and "LONG" in order_side:
            hold_side = "long"
        elif "OPEN" in order_side and "SHORT" in order_side:
            hold_side = "short"
        elif "CLOSE" in order_side and "LONG" in order_side:
            hold_side = "long"
        elif "CLOSE" in order_side and "SHORT" in order_side:
            hold_side = "short"

        if trigger_price and range_rate and amount and range_rate and clientOid:
            msg = "!!!!! EXECUTE TRAILLING TPSL ORDER !!!!!" + "\n"
            msg += "{} size: {} sltp trailling price: {}".format(order_side, amount, trigger_price) + "\n"

            exit_tpsl = False
            while not exit_tpsl:
                if not exit_tpsl and (hold_side == "long" or hold_side == "short"):
                    trigger_price = self.get_values([symbol])
                    value = trigger_price.loc[trigger_price["symbols"] == symbol, "values"].values[0]

                    lst_trigger_price = self.generateRangePrices(symbol, value, 0.2, 1000, 10)

                    for trigger_price in lst_trigger_price:
                        transaction = self.place_tpsl_order(symbol, marginCoin="USDT",
                                                            planType="moving_plan", triggerPrice=trigger_price,
                                                            holdSide=hold_side, triggerType="mark_price",
                                                            size=amount, rangeRate=range_rate, clientOid=clientOid)

                        try:
                            orderId = transaction['data']['orderId']
                            print("price : ", value)
                            print("len(lst_trigger_price) : ", len(lst_trigger_price))
                            print("lst_trigger_price : ", lst_trigger_price)
                            print("transaction orderId : ", orderId)
                            print("trigger price : ", initial_trigger_price)
                            print("trailer trigger price : ", trigger_price)
                            exit_tpsl = True
                            break
                        except:
                            print("price : ", value)
                            print("amount : ", amount)
                            print("price * amount : ", str(float(value) * float(amount)))
                            print("len(lst_trigger_price) : ", len(lst_trigger_price))
                            print("lst_trigger_price : ", lst_trigger_price)
                            print("TPSL: FAILED - DO NOT PANIC - TRY AGAIN")
                            pass

        if "msg" in transaction and transaction["msg"] == "success" and "data" in transaction and len(transaction["data"]) > 0:
            orderId = transaction["data"]["orderId"]
            self.add_gridId_orderId(grid_id, orderId, strategy_id, trend=trend)
            transaction_failure = False
        else:
            transaction_failure = True
            msg += "TRADE FAILED: {} size: {} sltp_trigger_price: {}".format(order_side, amount, trigger_price) + "\n"
        self.log_trade = self.log_trade + msg.upper()

        if not transaction_failure:
            order["trade_status"] = "SUCCESS"
            order["orderId"] = orderId
        elif transaction_failure:
            order["trade_status"] = "FAILED"
            order["orderId"] = None
        else:
            order["trade_status"] = "UNKNOWN"
        del msg

        return order

    def execute_lst_triggers(self, lst_triggers):
        lst_result_triggers = []
        with ThreadPoolExecutor() as executor:
            futures = []
            for trigger in lst_triggers:
                futures.append(executor.submit(self.execute_trigger, trigger))

            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

            for future in futures:
                lst_result_triggers.append(future.result())
        return lst_result_triggers

    def execute_lst_sltp_trailling_orders(self, lst_orders):
        lst_result_orders = []
        with ThreadPoolExecutor() as executor:
            futures = []
            for order in lst_orders:
                futures.append(executor.submit(self.execute_sltp_trailling, order))

            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

            for future in futures:
                lst_result_orders.append(future.result())

        return lst_result_orders

    def execute_lst_cancel_orders(self, lst_orders):
        if lst_orders and len(lst_orders) > 0:
            order_ids = [d['orderId'] for d in lst_orders]
            lst_success_orderIds = self.execute_list_cancel_orders(lst_orders[0]["symbol"], order_ids)
            for order in lst_orders:
                if order["orderId"] in lst_success_orderIds:
                    order["trade_status"] = "SUCCESS"
                else:
                    order["trade_status"] = "FAILED"
        return lst_orders

    def execute_lst_cancel_sltp_orders(self, lst_orders):
        if len(lst_orders) > 0:
            # order_ids = [d['orderId'] for d in lst_orders]
            lst_success_orderIds = self.execute_list_cancel_sltp_orders(lst_orders[0]["symbol"], lst_orders)
            for order in lst_orders:
                if order["orderId"] in lst_success_orderIds:
                    order["trade_status"] = "SUCCESS"
                else:
                    order["trade_status"] = "FAILED"
        return lst_orders

    @authentication_required
    def execute_batch_orders(self, lst_orders):
        lst_success_trade = []
        lst_failed_trade = []

        lst_symbols_unique = list(set(order['symbol'] for order in lst_orders))
        lst_orders_by_symbol = [[order for order in lst_orders if order['symbol'] == symbol] for symbol in lst_symbols_unique]

        for lst_orders_symbol in lst_orders_by_symbol:
            if len(lst_orders_symbol) > 0:
                symbol = self._get_symbol(lst_orders_symbol[0]["symbol"])
                lst_orderList = []
                for order in lst_orders_symbol:
                    if order["type"] == 'OPEN_LONG_ORDER':
                        side = "open_long"
                        leverage = self.get_leverage_long(symbol)
                    elif order["type"] == 'OPEN_SHORT_ORDER':
                        side = "open_short"
                        leverage = self.get_leverage_short(symbol)
                    elif order["type"] == 'CLOSE_LONG_ORDER':
                        side = "close_long"
                        leverage = self.get_leverage_long(symbol)
                    elif order["type"] == 'CLOSE_SHORT_ORDER':
                        side = "close_short"
                        leverage = self.get_leverage_short(symbol)

                    clientOid = self.clientOIdprovider.get_name(symbol, order["type"] + "__" + str(order["grid_id"]) + "__" + "--" + str(order["strategy_id"]) + "--")
                    size = utils.normalize_size(order["gross_size"] * leverage,
                                                self.get_sizeMultiplier(symbol))
                    orderParam = {
                        "size": str(size),
                        "price": str(order["price"]),
                        "side": side,
                        "orderType": "limit",
                        "timeInForceValue": "normal",
                        "clientOid": str(clientOid),
                    }
                    lst_orderList.append(orderParam)

            msg = "!!!!! EXECUTE BATCH LIMIT ORDER x" + str(len(lst_orderList)) + " !!!!!" + "\n"
            try:
                transaction = self._batch_orders_api(symbol, "USDT", lst_orderList)
            except (exceptions.BitgetAPIException, Exception) as e:
                transaction = None

            # Convert each dictionary to a string with newline character and concatenate them
            keys_to_exclude = ['orderType', 'timeInForceValue', 'clientOid']  # List of keys you want to exclude
            result_string = '\n'.join([
                json.dumps({k: v for k, v in orderParam.items() if k not in keys_to_exclude}).replace('"', '')
                # Remove all double quotes
                for orderParam in lst_orderList
            ])
            result_string = result_string.replace("{", "")
            result_string = result_string.replace("}", "")
            result_string = result_string.replace(",", "")
            msg += result_string + "\n"

            if transaction != None:
                if "msg" in transaction and transaction["msg"] == "success" and "data" in transaction and "orderInfo" in transaction["data"]:
                    if len(transaction["data"]["orderInfo"]) > 0:
                        if len(transaction["data"]["orderInfo"]) != len(lst_orderList):
                            msg += "SUCCESS TRADE:" + str(len(transaction["data"]["orderInfo"])) + " / " + str(len(lst_orderList)) + "\n"
                            failed_trade = len(lst_orderList) - len(transaction["data"]["orderInfo"])
                        else:
                            failed_trade = 0
                    for orderInfo in transaction["data"]["orderInfo"]:
                        orderId = orderInfo["orderId"]
                        gridId = [int(num) for num in re.findall(r'__(\d+)__', orderInfo["clientOid"])]
                        strategyId = orderInfo["clientOid"].split('--')[1]
                        self.add_gridId_orderId(gridId[0], orderId, strategyId, trend=None)
                        lst_success_trade.append({"orderId": orderId, "gridId": gridId[0], "strategyId": strategyId})
                        if failed_trade != 0:
                            msg += "success gridId: " + str(gridId[0]) + "\n"

                    if len(transaction["data"]["failure"]) > 0:
                        msg += "FAILED TRADE: " + str(len(transaction["data"]["failure"])) + "\n"
                        for failureInfo in transaction["data"]["failure"]:
                            gridId = [int(num) for num in re.findall(r'__(\d+)__', failureInfo["clientOid"])]
                            lst_failed_trade.append({"orderId": None, "gridId": gridId[0]})
                            msg += "failure gridId: " + str(gridId[0]) + "\n"
                            msg += "errorCode: " + failureInfo["errorCode"] + "\n"
                            msg += "errorMsg" + failureInfo["errorMsg"] + "\n"
                else:
                    msg += "TRADE BATCH FAILED" + "\n"
                    if "data" in transaction and "failure" in transaction["data"]:
                        if len(transaction["data"]["failure"]) > 0:
                            msg += "FAILED TRADE: " + str(len(transaction["data"]["failure"])) + " / " + str(len(lst_orderList)) + "\n"
                            for failureInfo in transaction["data"]["failure"]:
                                gridId = [int(num) for num in re.findall(r'__(\d+)__', failureInfo["clientOid"])]
                                strategyId = failureInfo["clientOid"].split('--')[1]
                                lst_failed_trade.append({"orderId": None, "gridId": gridId[0], "strategyId": strategyId})
                                msg += "failure gridId: " + str(gridId[0]) + "\n"
                                msg += "errorCode: " + failureInfo["errorCode"] + "\n"
                                msg += "errorMsg" + failureInfo["errorMsg"] + "\n"

                self.log_trade = self.log_trade + msg.upper()
                del msg

        success_trade_dict = {trade["gridId"]: trade["orderId"] for trade in lst_success_trade}

        if transaction == None:
            success_trade_dict = {}
            lst_failed_trade = []

        # Process each order in lst_orders
        for order in lst_orders:
            grid_id = order["grid_id"]
            if grid_id in success_trade_dict:
                order["trade_status"] = "SUCCESS"
                order["orderId"] = success_trade_dict[grid_id]
            elif grid_id in lst_failed_trade:
                order["trade_status"] = "FAILED"
                order["orderId"] = None
            else:
                order["trade_status"] = "UNKNOWN"
        return lst_orders

    def execute_trades_scenario(self, lst_orders):
        if lst_orders is None:
            return []
        if len(lst_orders) == 0:
            return lst_orders
        else:
            for order in lst_orders:
                grid_id = order["grid_id"]
                # if grid_id in success_trade_dict:
                if True:
                    order["trade_status"] = "SUCCESS"
                    order["orderId"] = "ORDER_ID_" + str(grid_id)
                elif grid_id in lst_failed_trade:
                    order["trade_status"] = "FAILED"
                    order["orderId"] = None
                else:
                    order["trade_status"] = "MISSING"
            return lst_orders

    def execute_batch_cancel_orders(self, symbol, lst_ordersIds):
        result = None
        if symbol != "" and len(lst_ordersIds) > 0:
            symbol = self._get_symbol(symbol)
            result = self._batch_cancel_orders_api(symbol, "USDT", lst_ordersIds)
        return result

    def execute_list_cancel_orders(self, symbol, lst_ordersIds):
        success_order_ids = []
        if symbol != "" and len(lst_ordersIds) > 0:
            symbol = self._get_symbol(symbol)
            result = self._cancel_Plan_Order_v2(symbol, "USDT", lst_ordersIds)
            if result == None:
                return []
            if result["msg"] == 'success':
                success_order_ids = [d['orderId'] for d in result["data"]["successList"]]
                # failure_order_ids = [d['orderId'] for d in result["data"]["failureList"]]

        return success_order_ids

    def execute_list_cancel_sltp_orders(self, symbol, lst_sltp_orders):
        success_order_ids = []
        if symbol != "" and len(lst_sltp_orders) > 0:
            symbol = self._get_symbol(symbol)
            for sltp_order in lst_sltp_orders:
                result = self._cancel_Plan_Order_v1(symbol, "USDT", sltp_order["orderId"], sltp_order["planType"])
                if "msg" in result \
                        and result["msg"] == 'success' \
                        and "data" in result \
                        and "orderId" in result["data"]:
                    success_order_ids.append(result["data"]["orderId"])
                else:
                    print("FAILURE CANCEL SLTP")

        return success_order_ids

    def execute_record_missing_orders(self, lst_record_missing_orders):
        for order in lst_record_missing_orders:
            self.add_gridId_orderId(order["gridId"], order["orderId"], order["strategyId"], order["trend"])

    def execute_cancel_sltp_orders(self, lst_cancel_sltp_orders):
        if len(lst_cancel_sltp_orders) > 0:
            self.execute_lst_cancel_sltp_orders(lst_cancel_sltp_orders)

    def execute_lst_orders(self, lst_open_orders):
        lst_result_open_orders = []
        with ThreadPoolExecutor() as executor:
            futures = []
            for open_order in lst_open_orders:
                futures.append(executor.submit(self.execute_order, open_order))

            wait(futures, timeout=1000, return_when=ALL_COMPLETED)

            for future in futures:
                lst_result_open_orders.append(future.result())

        return lst_result_open_orders

    def execute_order(self, trade):
        if trade["type"] in ["open_long", "open_short", "close_long", "close_short"]:
            symbol = trade["symbol"]
            clientOid = self.clientOIdprovider.get_name(symbol, trade["type"])
            trigger_price = self.get_values([trade["symbol"]])
            value = trigger_price.loc[trigger_price["symbols"] == symbol, "values"].values[0]
            if trade["type"] == "open_long":
                leverage = self.get_leverage_long(self._get_symbol(symbol))
                if trade["amount"] == None:
                    size = trade["gross_size"]
                else:
                    size = trade["amount"] / value
                size = size * leverage
                size = utils.normalize_size(size, self.get_sizeMultiplier(symbol))
                transaction = self._open_long_position(self._get_symbol(symbol), size, clientOid)
                trade["size"] = size
                trade["buying_price"] = value
            elif trade["type"] == "open_short":
                leverage = self.get_leverage_short(self._get_symbol(symbol))
                if trade["amount"] == None:
                    size = trade["gross_size"]
                else:
                    size = trade["amount"] / value
                size = size * leverage
                size = utils.normalize_size(size, self.get_sizeMultiplier(symbol))
                transaction = self._open_short_position(self._get_symbol(symbol), size, clientOid)
                trade["size"] = size
                trade["buying_price"] = value
            elif trade["type"] == "close_long":
                transaction = self._close_long_position(self._get_symbol(symbol), trade["size"], clientOid)
                trade["selling_price"] = value
            elif trade["type"] == "close_short":
                transaction = self._close_short_position(self._get_symbol(symbol), trade["size"], clientOid)
                trade["selling_price"] = value

            if "msg" in transaction \
                    and transaction["msg"] == "success" \
                    and "data" in transaction \
                    and "orderId" in transaction["data"]:
                trade["trade_status"] = "SUCCESS"
                trade["orderId"] = transaction["data"]["orderId"]
            else:
                trade["trade_status"] = "FAILED"

        return trade

    def execute_limit_orders(self, lst_orders):
        lst_result_orders = []
        max_batch_size = 49
        if len(lst_orders) > max_batch_size:
            sub_lst_orders = utils.split_list(lst_orders, max_batch_size)
            print("orders bach over max_batch_size")
            for lst in sub_lst_orders:
                lst_result_orders += self.execute_batch_orders(lst)
        elif len(lst_orders) >= 1:
            lst_result_orders = self.execute_batch_orders(lst_orders)
        return lst_result_orders

    @authentication_required
    def execute_trades(self, lst_orders):
        lst_orders = [order for order in lst_orders if order != None]
        if len(lst_orders) == 0:
            # self.execute_timer.set_time_to_zero("broker", "execute_trades", "execute_batch_orders", self.iter_execute_trades)
            self.iter_executetrades += 1
            return

        lst_result_open_orders = []
        lst_open_orders = [order for order in lst_orders if "trigger_type" in order and (order["trigger_type"] == "MARKET_OPEN_POSITION"
                                                                                         or order["trigger_type"] == "MARKET_CLOSE_POSITION")]
        lst_result_open_orders = self.execute_lst_orders(lst_open_orders)
        lst_orders = [order for order in lst_orders if not ("trigger_type" in order) or (order["trigger_type"] != "MARKET_OPEN_POSITION"
                                                                                         and order["trigger_type"] != "MARKET_CLOSE_POSITION")]

        lst_result_cancel_orders = []
        lst_cancel_orders = [order for order in lst_orders if "trigger_type" in order and order["trigger_type"] == "CANCEL_TRIGGER"]
        lst_result_cancel_orders = self.execute_lst_cancel_orders(lst_cancel_orders)
        lst_orders = [order for order in lst_orders if not ("trigger_type" in order) or order["trigger_type"] != "CANCEL_TRIGGER"]

        lst_result_triggers_orders = []
        lst_triggers = [order for order in lst_orders if "trigger_type" in order and order["trigger_type"] == "TRIGGER"]
        lst_result_triggers_orders = self.execute_lst_triggers(lst_triggers)
        lst_orders = [order for order in lst_orders if not ("trigger_type" in order) or order["trigger_type"] != "TRIGGER"]

        lst_result_sltp_trailling_orders = []
        lst_sltp_trailing_orders = [order for order in lst_orders if "trigger_type" in order and order["trigger_type"] == "SL_TP_TRAILER"]
        lst_result_sltp_trailling_orders = self.execute_lst_sltp_trailling_orders(lst_sltp_trailing_orders)
        lst_orders = [order for order in lst_orders if not ("trigger_type" in order) or order["trigger_type"] != "SL_TP_TRAILER"]

        lst_record_missing_orders = [order for order in lst_orders if "type" in order and order["type"] == "RECORD_DATA"]
        self.execute_record_missing_orders(lst_record_missing_orders)
        lst_orders = [order for order in lst_orders if not ("type" in order) or order["type"] != "RECORD_DATA"]

        lst_cancel_sltp_orders = [order for order in lst_orders if "type" in order and order["type"] == "CANCEL_SLTP"]
        self.execute_cancel_sltp_orders(lst_cancel_sltp_orders)
        lst_orders = [order for order in lst_orders if not ("type" in order) or order["type"] != "CANCEL_SLTP"]

        lst_result_orders = self.execute_limit_orders(lst_orders)

        # self.execute_timer.set_start_time("broker", "execute_trades", "execute_batch_orders", self.iter_execute_trades)
        del lst_triggers
        del lst_orders
        locals().clear()
        self.iter_execute_trades += 1

        return lst_result_orders \
               + lst_result_triggers_orders \
               + lst_result_sltp_trailling_orders \
               + lst_result_cancel_orders \
               + lst_result_open_orders

    def set_open_orders_gridId(self, df_open_orders):
        df_open_orders["gridId"] = None
        df_open_orders["strategyId"] = None
        df_open_orders["trend"] = None
        for orderId in df_open_orders["orderId"].tolist():
            # Define a condition
            condition = df_open_orders['orderId'] == orderId
            # Set a value in the 'gridId' column where the condition is true
            res = self.get_gridId_from_orderId(orderId)
            df_open_orders.loc[condition, 'gridId'] = res
            res = self.get_strategyId_from_orderId(orderId)
            df_open_orders.loc[condition, 'strategyId'] = res
            res = self.get_trend_from_orderId(orderId)
            df_open_orders.loc[condition, 'trend'] = res
            del condition
            del res
        self.iter_set_open_orders_gridId += 1
        return df_open_orders

    def get_gridId_from_orderId(self, orderId):
        condition = self.df_grid_id_match['orderId'] == orderId
        if condition.any() and orderId == self.df_grid_id_match.at[condition.idxmax(), 'orderId']:
            grid_id = self.df_grid_id_match.at[condition.idxmax(), 'grid_id']
            del condition
            return grid_id
        else:
            msg = "ERROR: BROKER ID NOT FOUND" + "\n"
            msg += "ORDER ID: " + str(orderId) + "\n"
            self.log_trade = self.log_trade + msg.upper()
            del msg
            del condition
            return None

    def get_strategyId_from_orderId(self, orderId):
        condition = self.df_grid_id_match['orderId'] == orderId
        if condition.any() and orderId == self.df_grid_id_match.at[condition.idxmax(), 'orderId']:
            strategy_id = self.df_grid_id_match.at[condition.idxmax(), 'strategy_id']
            del condition
            return strategy_id
        else:
            msg = "ERROR: BROKER ID NOT FOUND" + "\n"
            msg += "ORDER ID: " + str(orderId) + "\n"
            self.log_trade = self.log_trade + msg.upper()
            del msg
            del condition
            return None

    def get_trend_from_orderId(self, orderId):
        condition = self.df_grid_id_match['orderId'] == orderId
        if condition.any() and orderId == self.df_grid_id_match.at[condition.idxmax(), 'orderId']:
            strategy_id = self.df_grid_id_match.at[condition.idxmax(), 'trend']
            del condition
            return strategy_id
        else:
            msg = "ERROR: BROKER ID NOT FOUND" + "\n"
            msg += "ORDER ID: " + str(orderId) + "\n"
            self.log_trade = self.log_trade + msg.upper()
            del msg
            del condition
            return None

    def export_history(self, target):
        pass

    def _build_df_open_positions(self, open_positions):
        df_open_positions = pd.DataFrame(columns=["symbol", "holdSide", "leverage", "marginCoin",
                                                  "available", "total", "usdtEquity",
                                                  "marketPrice", "averageOpenPrice",
                                                  "achievedProfits", "unrealizedPL", "liquidationPrice"])
        for i in range(len(open_positions)):
            data = open_positions[i]
            df_open_positions.loc[i] = pd.Series({"symbol": data["symbol"], "holdSide": data["holdSide"], "leverage": data["leverage"], "marginCoin": data["marginCoin"],
                                                  "available": float(data["available"]), "total": float(data["total"]), "usdtEquity": float(data["margin"]),
                                                  "marketPrice": float(data["marketPrice"]),  "averageOpenPrice": float(data["averageOpenPrice"]),
                                                  "achievedProfits": float(data["achievedProfits"]), "unrealizedPL": float(data["unrealizedPL"]), "liquidationPrice": float(data["liquidationPrice"])
                                                  })
        return df_open_positions

    def _build_df_open_orders(self, open_orders):
        df_open_orders = pd.DataFrame(columns=["symbol", "price", "side", "size", "leverage", "marginCoin", "marginMode", "reduceOnly", "clientOid", "orderId"])
        for i in range(len(open_orders)):
            data = open_orders[i]
            # df_open_orders.loc[i] = pd.Series({"symbol": data["symbol"], "price": data["price"], "side": data["side"], "size": data["size"], "leverage": data["leverage"], "marginCoin": data["marginCoin"], "marginMode": data["marginMode"], "reduceOnly": data["reduceOnly"], "clientOid": data["clientOid"], "orderId": data["orderId"]})
            df_open_orders.loc[i] = pd.Series({
                "symbol": data["symbol"],
                "price": data["price"],
                "side": data["side"],
                "size": data["size"],
                "leverage": data["leverage"],
                "marginCoin": data["marginCoin"],
                "marginMode": data["marginMode"],
                "reduceOnly": bool(data["reduceOnly"]),  # Explicitly cast to bool
                "clientOid": data["clientOid"],
                "orderId": data["orderId"]
            })
            df_open_orders["reduceOnly"] = df_open_orders["reduceOnly"].astype(bool)
        return df_open_orders

    def _build_df_triggers(self, triggers):
        df_triggers = pd.DataFrame(columns=["planType", "symbol", "size", "side", "orderId", "orderType", "clientOid",
                                            "price", "triggerPrice", "triggerType", "marginMode",
                                            "gridId", "strategyId", "trend",
                                            "executeOrderId", "planStatus"])
        for i in range(len(triggers)):
            data = triggers[i]
            grid_id = self.get_gridId_from_orderId(data["orderId"])
            if grid_id == None:
                grid_id = 0
                strategyId = None
                trend = None
            else:
                strategyId = self.get_strategyId_from_orderId(data["orderId"])
                trend = self.get_trend_from_orderId(data["orderId"])
            df_triggers.loc[i] = pd.Series({
                "planType": data["planType"],
                "symbol": data["symbol"],
                "size": data["size"],
                "side": data["side"],
                "orderId": data["orderId"],
                "orderType": data["orderType"],
                "clientOid": data["clientOid"],
                "price": data["price"],
                "triggerPrice": data["triggerPrice"],
                "triggerType": data["triggerType"],
                "marginMode": data["marginMode"],
                "gridId": grid_id,
                "strategyId": strategyId,
                "trend": trend,
                "executeOrderId": "",
                "planStatus": ""
            })
        return df_triggers

    def _build_df_orders_plan_history(self, orders):
        df_orders = pd.DataFrame(columns=["orderId", "clientOid", "executeOrderId", "planType", "planStatus"])
        for i in range(len(orders)):
            data = orders[i]
            df_orders.loc[i] = pd.Series({
                "orderId": data["orderId"],
                "clientOid": data["clientOid"],
                "executeOrderId": data["executeOrderId"],
                "planType": data["planType"],
                "planStatus": data["planStatus"]
            })
        return df_orders

    @authentication_required
    def execute_reset_account(self):
        df_positions = self.get_open_position()
        original_df_positions = df_positions
        #return original_df_positions

        if len(df_positions) == 0:
            usdtEquity = self.get_account_equity()
            self.log("reset - no position - account already cleared")
            self.log('equity USDT: {}'.format(usdtEquity))
            return original_df_positions

        original_df_positions = df_positions

        for symbol in df_positions['symbol'].tolist():
            size = df_positions.loc[(df_positions['symbol'] == symbol), "total"].values[0]
            holdSize = df_positions.loc[(df_positions['symbol'] == symbol), "holdSide"].values[0]
            if holdSize == 'long':
                clientOid = self.clientOIdprovider.get_name(symbol, "close_long")
                transaction = self._close_long_position(symbol, size, clientOid)
            elif holdSize == 'short':
                clientOid = self.clientOIdprovider.get_name(symbol, "close_short")
                transaction = self._close_long_position(symbol, size, clientOid)
            if "msg" in transaction \
                    and transaction["msg"] == "success" \
                    and "data" in transaction \
                    and "orderId" in transaction["data"]:
                usdtEquity = df_positions.loc[(df_positions['symbol'] == symbol), "usdtEquity"].values[0]
                self.log('reset - close ' + str(holdSize) + 'position - symbol: ' + symbol + ' value: ' + str(size) + ' - $' + str(usdtEquity))

        df_positions = self.get_open_position()
        if len(df_positions) != 0:
            self.log("reset - failure")
            exit(532)
        else:
            usdtEquity = self.get_account_equity()
            self.log('reset - account cleared')
            self.log('equity USDT: {}'.format(usdtEquity))

        return original_df_positions

    @authentication_required
    def get_symbol_unrealizedPL(self, symbol):
        df_positions = self.get_open_position()
        try:
            unrealizedPL = df_positions.loc[(df_positions['symbol'] == symbol), "unrealizedPL"].values[0]
        except:
            self.log("error: get_symbol_unrealizedPL {}".format(len(df_positions)))
            unrealizedPL = 0
        del df_positions
        return unrealizedPL

    @authentication_required
    def get_symbol_holdSide(self, symbol):
        df_positions = self.get_open_position()
        try:
            holdSide = df_positions.loc[(df_positions['symbol'] == symbol), "holdSide"].values[0]
        except:
            self.log("error: get_symbol_holdSide {}".format(len(df_positions)))
            holdSide = ""
        del df_positions
        return holdSide

    @authentication_required
    def get_symbol_averageOpenPrice(self, symbol):
        df_positions = self.get_open_position()
        averageOpenPrice = df_positions.loc[(df_positions['symbol'] == symbol), "averageOpenPrice"].values[0]
        df_positions = None
        return averageOpenPrice

    @authentication_required
    def get_symbol_marketPrice(self, symbol):
        df_positions = self.get_open_position()
        marketPrice = df_positions.loc[(df_positions['symbol'] == symbol), "marketPrice"].values[0]
        df_positions = None
        return marketPrice

    @authentication_required
    def get_symbol_total(self, symbol):
        df_positions = self.get_open_position()
        total = 0
        if symbol in df_positions['symbol'].tolist():
            total = df_positions.loc[(df_positions['symbol'] == symbol), "total"].values[0]
        df_positions = None
        return total

    @authentication_required
    def get_symbol_available(self, symbol):
        df_positions = self.get_open_position()
        available = 0
        if symbol in df_positions['symbol'].tolist():
            available = df_positions.loc[(df_positions['symbol'] == symbol), "available"].values[0]
        df_positions = None
        return available

    @authentication_required
    def get_symbol_data(self, symbol):
        df_positions = self.get_open_position()
        if symbol in df_positions['symbol'].tolist():
            available = df_positions.loc[(df_positions['symbol'] == symbol), "available"].values[0]
            total = df_positions.loc[(df_positions['symbol'] == symbol), "total"].values[0]
            leverage = df_positions.loc[(df_positions['symbol'] == symbol), "leverage"].values[0]
            marketPrice = df_positions.loc[(df_positions['symbol'] == symbol), "marketPrice"].values[0]
            averageOpenPrice = df_positions.loc[(df_positions['symbol'] == symbol), "averageOpenPrice"].values[0]
            unrealizedPL = df_positions.loc[(df_positions['symbol'] == symbol), "unrealizedPL"].values[0]
            liquidation = df_positions.loc[(df_positions['symbol'] == symbol), "liquidationPrice"].values[0]
            side = df_positions.loc[(df_positions['symbol'] == symbol), "holdSide"].values[0]
            df_positions = None
            return total, available, leverage, averageOpenPrice, marketPrice, unrealizedPL, liquidation, side
        else:
            df_positions = None
            return 0, 0, 0, 0, 0, 0, 0, ""

    @authentication_required
    def get_symbol_usdtEquity(self, symbol):
        df_positions = self.get_open_position()
        usdtEquity = df_positions.loc[(df_positions['symbol'] == symbol), "usdtEquity"].values[0]
        df_positions = None
        return usdtEquity

    @authentication_required
    def get_lst_symbol_position(self):
        df_positions = self.get_open_position()
        lst_positions = df_positions['symbol'].tolist()
        df_positions = None
        return lst_positions

    @authentication_required
    def get_global_unrealizedPL(self):
        df_positions = self.get_open_position()
        global_unrealizedPL = 0
        if len(df_positions) > 0:
            global_unrealizedPL = df_positions["unrealizedPL"].sum()
        df_positions = None
        return global_unrealizedPL

    def get_coin_from_symbol(self, symbol):
        return self._get_coin(symbol)
