from . import broker,trade,rtdp,utils
import pandas as pd
import gc
import re
import json
from . bitget import exceptions
from concurrent.futures import wait, ALL_COMPLETED, ThreadPoolExecutor
import threading

class BrokerBitGet(broker.Broker):
    def __init__(self, params = None):
        super().__init__(params)
        self.rtdp = rtdp.RealTimeDataProvider(params)
        self.simulation = False
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
            self.simulation = params.get("simulation", self.simulation)
            if self.simulation == 0 or self.simulation == "0":
                self.simulation = False
            if self.simulation == 1 or self.simulation == "1":
                self.simulation = True

        #if not self._authentification():
        #    print("[BrokerBitGet] : Problem encountered during authentification")

        self.clientOIdprovider = utils.ClientOIdProvider()

        self.df_grid_id_match = pd.DataFrame(columns=["orderId", "grid_id", "strategy_id", "trend"])
        self.lock = threading.Lock()
        self.lock_bitget = threading.Lock()
        self.execute_timer = None
        self.iter_execute_orders = 0
        self.iter_set_open_orders_gridId = 0

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

        self.iter_execute_orders = 0
        self.iter_set_open_orders_gridId = 0

    @authentication_required
    def set_margin_mode_and_leverages(self, df_margin_mode_leverages):
        for index, row in df_margin_mode_leverages.iterrows():
            symbol = row["symbol"]
            self.set_symbol_margin(symbol, row["margin_mode"])
            self.set_symbol_leverage(symbol, row["leverage_long"], "long")
            self.set_symbol_leverage(symbol, row["leverage_short"], "short")
        del df_margin_mode_leverages

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

    @authentication_required
    def execute_trade(self, trade):
        self.log("!!!!!!! EXECUTE THE TRADE !!!!!!!")
        if trade.time != None:
            self.log("execute trade at: {}".format(trade.time))
        trade.success = False
        self.trade_symbol = self._get_symbol(trade.symbol)

        #self.set_margin_and_leverage(self.trade_symbol)
        self.clientOid = self.clientOIdprovider.get_name(self.trade_symbol, trade.type)

        self.log("TRADE GROSS SIZE: {}".format(trade.gross_size))
        trade.gross_size = self.normalize_size(self.trade_symbol, trade.gross_size)

        self.log("TRADE GROSS SIZE NORMALIZED: {}".format(trade.gross_size))
        if hasattr(trade, 'price'):
            self.log("TRADE GROSS PRICE: {}".format(trade.price))
            trade.price = self.normalize_price(self.trade_symbol, trade.price) # price not used yet
            self.log("TRADE GROSS PRICE NORMALIZED: {}".format(trade.price))

        if trade.gross_size == 0:
            self.log('transaction failed ", trade.type, " : ' + self.trade_symbol + ' - gross_size: ' + str(trade.gross_size))

        trigger_params = None
        if hasattr(trade, "trigger_price"):
            trigger_params = {}
            trigger_params["trigger_price"] = trade.trigger_price
        if trade.type in ["OPEN_LONG", "OPEN_SHORT", "OPEN_LONG_ORDER", "OPEN_SHORT_ORDER", "CLOSE_LONG_ORDER", "CLOSE_SHORT_ORDER"]:
            if trade.type == "OPEN_LONG":
                self.log("{} size: {}".format(trade.type, trade.gross_size))
                transaction = self._open_long_position(self.trade_symbol, trade.gross_size, self.clientOid)
            elif trade.type == "OPEN_SHORT":
                self.log("{} size: {}".format(trade.type, trade.gross_size))
                transaction = self._open_short_position(self.trade_symbol, trade.gross_size, self.clientOid)
            elif trade.type == "OPEN_LONG_ORDER":
                self.log("{} size: {} price: {}".format(trade.type, trade.gross_size, trade.price))
                transaction = self._open_long_order(self.trade_symbol, trade.gross_size, self.clientOid, trade.price, trigger_params)
            elif trade.type == "OPEN_SHORT_ORDER":
                self.log("{} size: {} price: {}".format(trade.type, trade.gross_size, trade.price))
                transaction = self._open_short_order(self.trade_symbol, trade.gross_size, self.clientOid, trade.price, trigger_params)
            elif trade.type == "CLOSE_LONG_ORDER":
                self.log("{} size: {} price: {}".format(trade.type, trade.gross_size, trade.price))
                transaction = self._close_long_order(self.trade_symbol, trade.gross_size, self.clientOid, trade.price, trigger_params)
            elif trade.type == "CLOSE_SHORT_ORDER":
                self.log("{} size: {} price: {}".format(trade.type, trade.gross_size, trade.price))
                transaction = self._close_short_order(self.trade_symbol, trade.gross_size, self.clientOid, trade.price, trigger_params)
            else:
                transaction = {"msg": "failure"}

            self.log(str(transaction))
            if "msg" in transaction and transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                if "_ORDER" in trade.type:
                    trade.symbol_price = trade.price
                    trade.tradeId = trade.orderId
                else:
                    trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.buying_fee = self.get_order_fill_detail(self.trade_symbol, trade.orderId)
                    trade.net_price = trade.gross_price
                    trade.bought_gross_price = trade.gross_price
                trade.net_size = trade.gross_size
                trade.buying_price = trade.symbol_price

                self.log("request " + trade.type + ": " + self.trade_symbol + " gross_size: " + str(trade.gross_size))
                self.log(trade.type + ": " + self.trade_symbol + " gross_size: " + str(trade.gross_size))
                if hasattr(trade, 'gross_price') and hasattr(trade, 'buying_fee'):
                    self.log(" price: " + str(trade.gross_price) + " fee: " + str(trade.buying_fee))
            else:
                self.log("Something went wrong inside execute_trade : " + str(transaction))

            if  "data" in transaction:
                transaction["data"].clear()
                del transaction["data"]
            transaction.clear()
            del transaction

        elif trade.type == "CLOSE_LONG":
            trade.gross_size = self.get_symbol_total(self.trade_symbol)
            transaction = self._close_long_position(self.trade_symbol, trade.gross_size, self.clientOid)
            if "msg" in transaction and transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                self.log('request CLOSE_LONG: ' + self.trade_symbol + ' gross_size: ' + str(trade.gross_size))
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(self.trade_symbol, trade.orderId)
                self.log('CLOSE_LONG: ' + self.trade_symbol + ' gross_size: ' + str(trade.gross_size) + ' price: ' + str(trade.gross_price) + ' fee: ' + str(trade.selling_fee))
                # CEDE to be confirmed : selling_fee is selling_fee + buying_fee or just selling_fee
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                if hasattr(trade, "bought_gross_price"):
                   # trade.roi = 100 * (trade.gross_price - trade.bought_gross_price - trade.selling_fee - trade.buying_fee) / trade.bought_gross_price
                   trade.roi = utils.get_variation(trade.bought_gross_price, trade.gross_price)
            if "data" in transaction:
                transaction["data"].clear()
                del transaction["data"]
            transaction.clear()
            del transaction

        elif trade.type == "CLOSE_SHORT":
            trade.gross_size = self.get_symbol_total(self.trade_symbol)
            transaction = self._close_short_position(self.trade_symbol, trade.gross_size, self.clientOid)
            if transaction["msg"] == "success" and "data" in transaction and "orderId" in transaction["data"]:
                trade.success = True
                trade.orderId = transaction["data"]["orderId"]
                trade.clientOid = transaction["data"]["clientOid"]
                self.log('request CLOSE_SHORT: ' + self.trade_symbol + ' gross_size: ' + str(trade.gross_size))
                trade.tradeId, trade.symbol_price, trade.gross_price, trade.gross_size, trade.selling_fee = self.get_order_fill_detail(self.trade_symbol, trade.orderId)
                self.log('CLOSE_SHORT: ' + self.trade_symbol + ' gross_size: ' + str(trade.gross_size) + ' price: ' + str(trade.gross_price) + ' fee: ' + str(trade.selling_fee))
                trade.net_size = trade.gross_size
                trade.net_price = trade.gross_price
                if hasattr(trade, "bought_gross_price"):
                    # trade.roi = 100 * (-1) * (trade.gross_price - trade.bought_gross_price - trade.selling_fee - trade.buying_fee) / trade.bought_gross_price
                    trade.roi = (-1) * utils.get_variation(trade.bought_gross_price, trade.gross_price)
            if "data" in transaction:
                transaction["data"].clear()
                del transaction["data"]
            transaction.clear()
            del transaction

        if not trade.success:
            msg = 'trade failed: ' + trade.symbol
            msg += " - " + trade.type
            if hasattr(trade, 'price'):
                msg += " - " + str(trade.price)
            msg += " - " + str(trade.gross_size) + '\n'
            self.log('transaction failed : ' + trade.symbol + " - type: " + trade.type + ' - gross_size: ', str(trade.gross_size))
            self.log("!!!!!!! EXECUTE THE TRADE FAILED !!!!!!!")
        else:
            msg = trade.symbol
            msg += " - " + trade.type
            if hasattr(trade, 'price'):
                msg += " - " + str(trade.price)
            msg += " - " + str(trade.gross_size) + '\n'
            self.log('transaction success : ' + trade.symbol + " - type: " + trade.type + ' - gross_size: ' + str(trade.gross_size))
            self.log("!!!!!!! EXECUTE THE TRADE COMPLETED !!!!!!!")
        self.log_trade = self.log_trade + msg.upper()
        del msg
        locals().clear()
        return trade.success

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
        with self.lock:
            self.df_grid_id_match.loc[len(self.df_grid_id_match)] = [orderId, gridId, strategyId, trend]
            self.df_grid_id_match = self.df_grid_id_match.drop_duplicates()

    def clear_gridId_orderId(self, lst_orderId):
        if False \
                and (len(self.df_grid_id_match) > len(lst_orderId)):
            self.df_grid_id_match = self.df_grid_id_match[self.df_grid_id_match['orderId'].isin(lst_orderId)]

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
        amount = str(trigger["gross_size"])
        if not isinstance(amount, str):
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


        clientOid = self.clientOIdprovider.get_name(symbol,
                                                    trigger["type"]
                                                    + "__" + str(trigger["grid_id"]) + "__"
                                                    + "--" + str(trigger["strategy_id"]) + "--")

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

            # amount = str(15)  # CEDE For test size / amount
            with self.lock_bitget:
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
            strategy_id = trigger["strategy_id"]
            self.add_gridId_orderId(grid_id, orderId, strategy_id, trend=trend)
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
        amount = str(order["gross_size"])
        if not isinstance(amount, str):
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

            # amount = str(15)  # CEDE For test  size / amount

            exit_tpsl = False
            while not exit_tpsl:
                if not exit_tpsl and (hold_side == "long" or hold_side == "short"):
                    trigger_price = self.get_values([symbol])
                    value = trigger_price.loc[trigger_price["symbols"] == symbol, "values"].values[0]

                    lst_trigger_price = self.generateRangePrices(symbol, value, 0.02, 100)

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
        if len(lst_orders) > 0:
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
        if len(lst_orders) > 0:
            symbol = self._get_symbol(lst_orders[0]["symbol"])
            lst_orderList = []
            leverage_short = self.get_leverage_short(symbol)
            leverage_long = self.get_leverage_long(symbol)
            for order in lst_orders:
                if order["type"] == 'OPEN_LONG_ORDER':
                    side = "open_long"
                    leverage = leverage_long
                elif order["type"] == 'OPEN_SHORT_ORDER':
                    side = "open_short"
                    leverage = leverage_short
                elif order["type"] == 'CLOSE_LONG_ORDER':
                    side = "close_long"
                    leverage = leverage_long
                elif order["type"] == 'CLOSE_SHORT_ORDER':
                    side = "close_short"
                    leverage = leverage_short

                clientOid = self.clientOIdprovider.get_name(symbol, order["type"] + "__" + str(order["grid_id"]) + "__" + "--" + str(order["strategy_id"]) + "--")
                orderParam = {
                    "size": str(order["gross_size"] * leverage),
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

        lst_success_trade = []
        lst_failed_trade = []
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

    def execute_orders_scenario(self, lst_orders):
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

    def get_pending_orders(self, symbol):
        if symbol != "":
            symbol = self._get_symbol(symbol)
            result = self._get_orders_Pending_v2(symbol)
            if result["msg"] == 'success':
                # Check if the list is empty
                if not(result["data"]['entrustedList'] is None):
                    # Convert the list of dictionaries into a DataFrame
                    df = pd.DataFrame(result["data"]['entrustedList'])

                    # Convert specific columns to appropriate types
                    df['size'] = df['size'].astype(int)
                    df['baseVolume'] = df['baseVolume'].astype(float)
                    df['fee'] = df['fee'].astype(float)
                    df['totalProfits'] = df['totalProfits'].astype(float)
                    df['quoteVolume'] = df['quoteVolume'].astype(float)
                    df['leverage'] = df['leverage'].astype(int)
                    df['presetStopLossPrice'] = df['presetStopLossPrice'].astype(float)
                    df['cTime'] = pd.to_datetime(df['cTime'], unit='ms')
                    df['uTime'] = pd.to_datetime(df['uTime'], unit='ms')

                    df['symbol'] = df['symbol'].str.rstrip('USDT') # CEDE better way to do it
                else:
                    columns = [
                        'symbol', 'size', 'orderId', 'clientOid', 'baseVolume', 'fee', 'price', 'priceAvg', 'status',
                        'side', 'force',
                        'totalProfits', 'posSide', 'marginCoin', 'quoteVolume', 'leverage', 'marginMode',
                        'enterPointSource', 'tradeSide',
                        'posMode', 'orderType', 'orderSource', 'presetStopSurplusPrice', 'presetStopLossPrice',
                        'reduceOnly', 'cTime', 'uTime'
                    ]
                    # Create an empty DataFrame with specified columns
                    df = pd.DataFrame(columns=columns)

        return df

    def execute_record_missing_orders(self, lst_record_missing_orders):
        for order in lst_record_missing_orders:
            self.add_gridId_orderId(order["gridId"], order["orderId"], order["strategyId"], order["trend"])

    def execute_cancel_sltp_orders(self, lst_cancel_sltp_orders):
        if len(lst_cancel_sltp_orders) > 0:
            print("[execute_cancel_sltp_orders] orders : ", lst_cancel_sltp_orders)
            result = self.execute_lst_cancel_sltp_orders(lst_cancel_sltp_orders)
            print("[execute_cancel_sltp_orders] result : ", result)

    @authentication_required
    def execute_orders(self, lst_orders):
        if len(lst_orders) == 0:
            # self.execute_timer.set_time_to_zero("broker", "execute_orders", "execute_batch_orders", self.iter_execute_orders)
            self.iter_execute_orders += 1
            return

        lst_result_cancel_orders = []
        lst_cancel_orders = [order for order in lst_orders if "trigger_type" in order and order["trigger_type"] == "CANCEL_TRIGGER"]
        lst_result_cancel_orders = self.execute_lst_cancel_orders(lst_cancel_orders)
        lst_orders = [order for order in lst_orders if not ("trigger_type" in order) or order["trigger_type"] != "CANCEL_TRIGGER"]

        lst_result_triggers_orders = []
        lst_triggers = [order for order in lst_orders if "trigger_type" in order and order["trigger_type"] == "TRIGGER"]
        lst_result_triggers_orders = self.execute_lst_triggers(lst_triggers)
        # extract orders
        lst_orders = [order for order in lst_orders if not ("trigger_type" in order) or order["trigger_type"] != "TRIGGER"]

        lst_result_sltp_trailling_orders = []
        # extract sltp trailling orders...
        lst_sltp_trailing_orders = [order for order in lst_orders if "trigger_type" in order and order["trigger_type"] == "SL_TP_TRAILER"]
        # ...and execute them
        lst_result_sltp_trailling_orders = self.execute_lst_sltp_trailling_orders(lst_sltp_trailing_orders)
        # extract orders
        lst_orders = [order for order in lst_orders if not ("trigger_type" in order) or order["trigger_type"] != "SL_TP_TRAILER"]

        lst_result_record_missing_orders = []
        lst_record_missing_orders = [order for order in lst_orders if "type" in order and order["type"] == "RECORD_DATA"]
        self.execute_record_missing_orders(lst_record_missing_orders)
        lst_orders = [order for order in lst_orders if not ("type" in order) or order["type"] != "RECORD_DATA"]

        lst_result_cancel_sltp_orders = []
        lst_cancel_sltp_orders = [order for order in lst_orders if "type" in order and order["type"] == "CANCEL_SLTP"]
        self.execute_cancel_sltp_orders(lst_cancel_sltp_orders)
        lst_orders = [order for order in lst_orders if not ("type" in order) or order["type"] != "CANCEL_SLTP"]

        lst_result_orders = []
        max_batch_size = 49
        if len(lst_orders) > max_batch_size:
            sub_lst_orders = utils.split_list(lst_orders, max_batch_size)
            print("orders bach over max_batch_size")
            for lst in sub_lst_orders:
                lst_result_orders += self.execute_batch_orders(lst)
        elif len(lst_orders) >= 1:
            lst_result_orders = self.execute_batch_orders(lst_orders)

        # self.execute_timer.set_start_time("broker", "execute_orders", "execute_batch_orders", self.iter_execute_orders)
        del lst_triggers
        del lst_orders
        del max_batch_size
        locals().clear()
        self.iter_execute_orders += 1

        return lst_result_orders \
               + lst_result_triggers_orders \
               + lst_result_sltp_trailling_orders \
               + lst_result_cancel_orders

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
            current_trade = trade.Trade()
            current_trade.symbol = symbol
            current_trade.gross_size = df_positions.loc[(df_positions['symbol'] == symbol), "total"].values[0]
            current_trade.type = "CLOSE_LONG"
            holdSize = df_positions.loc[(df_positions['symbol'] == symbol), "holdSide"].values[0]
            if holdSize == 'long':
                current_trade.type = "CLOSE_LONG"
            else:
                current_trade.type = "CLOSE_SHORT"
            current_trade.minsize = 0
            res = self.execute_trade(current_trade)
            if res:
                usdtEquity = df_positions.loc[(df_positions['symbol'] == symbol), "usdtEquity"].values[0]
                self.log('reset - close ' + str(holdSize) + 'position - symbol: ' + symbol + ' value: ' + str(current_trade.gross_size) + ' - $' + str(usdtEquity))

        df_positions = self.get_open_position()
        if len(df_positions) != 0:
            self.log("reset - failure")
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
