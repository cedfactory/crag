import zmq
import pandas as pd
from src import broker_bitget_api
# from bitget_ws import bitget_ws_account_tickers
from bitget_ws_account_data import WS_Account_Data, convert_open_orders_push_list_to_df, convert_triggers_convert_df_to_df
import bitget_ws_account_tickers

import time

class ZMQServer:
    def __init__(self, conf_data=None, bind_address="tcp://*:5555"):
        """Initialize a ZeroMQ server with a REP socket."""
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        # Bind the REP socket to the specified address (all interfaces on port 5555 by default)
        self.socket.bind(bind_address)
        print(f"Server bound to {bind_address}")
        if not conf_data is None:
            account_id = conf_data.get("account_id","")
            self.lst_symbols = conf_data.get("symbols", [])
            ticker_symbols = {symbol.removesuffix("_UMCBL") for symbol in self.lst_symbols} # CEDE DODGY USE GET_SYMBOL

            api_key = conf_data.get("account_keys", {}).get("api_key")
            api_secret = conf_data.get("account_keys", {}).get("api_secret")
            api_password = conf_data.get("account_keys", {}).get("api_password")

            params = {
                "account_id": account_id,
                "tickers": ticker_symbols,
                "api_key": api_key,
                "api_secret": api_secret,
                "api_passphrase": api_password
            }

            self.ws_client = bitget_ws_account_tickers.WSAccountTickers(params)

            timeout = 30  # seconds
            start_time = time.time()
            while self.ws_client.get_status() == "Off":
                if time.time() - start_time > timeout:
                    print("Timeout waiting for ws_client to start")
                    self.ws_client.stop()
                    exit(322)
                time.sleep(1)

            status = self.ws_client.get_status()
            if status == "Failed":
                print("ws_client failed")
                self.ws_client.stop()
                exit(321)
            elif status == "On":
                print("ws_client is On")

            my_broker = broker_bitget_api.BrokerBitGetApi({"account": account_id, "reset_account_start": False})

            df_triggers_histo = my_broker.get_all_triggers(by_pass=True)
            df_open_position_histo = my_broker.get_open_position(by_pass=True)
            del my_broker

            self.ws_data = WS_Account_Data(df_open_positions=df_open_position_histo, df_triggers=df_triggers_histo)

    def start(self):
        """Start the server loop to listen for requests and send replies."""
        try:
            while True:
                # SET WS DATA
                self.ws_current_state = self.ws_client.get_state()

                self.ws_data.set_ws_prices(
                    self.ws_current_state["ticker_prices"]
                )
                self.ws_data.set_ws_account(
                    self.ws_current_state["account"]
                )

                df_trigger = convert_triggers_convert_df_to_df(self.ws_current_state["orders-algo"])
                # df_trigger = self.set_open_orders_gridId(df_trigger)
                self.ws_data.set_ws_triggers(df_trigger)

                df_open_order = convert_open_orders_push_list_to_df(self.ws_current_state["orders"])
                # df_open_order = self.set_open_orders_gridId(df_open_order)
                self.ws_data.set_ws_open_orders(df_open_order)

                df_open_position = self._build_df_open_positions_ws(self.ws_current_state["positions"],
                                                                    self.ws_data.get_ws_df_prices())
                self.ws_data.set_ws_open_positions(df_open_position)


                # Wait for the next client request (blocks until a request arrives)
                message = self.socket.recv_json()  # Receive data in JSON format (deserializes to Python dict)
                print("Server received message:", message)

                # Deserialize the message content to a Python object
                data = self._deserialize_message(message)

                # Process the data and prepare a response
                response_data = self.handle_request(data)

                response_status = ["GET_STATUS"]
                if isinstance(response_data, dict) and "action" in response_data and "request" in response_data:
                    if response_data["action"] == "INFO" and response_data["request"] == "GET_STATUS":
                        if self.ws_client.get_status() == "On" \
                                and self.ws_data.get_data_status():
                            response_data = {
                                "status": self.ws_client.get_status()
                            }
                        if self.ws_client.get_status() == "Failed":
                            response_data = {
                                "status": self.ws_client.get_status()
                            }

                request_handlers = {
                    "TRIGGERS": self.handle_triggers,
                    "OPEN_POSITIONS": self.handle_open_positions,
                    "OPEN_ORDERS": self.handle_open_orders,
                    "ACCOUNT": self.handle_account,
                    "PRICES": self.handle_prices,
                    "USDT_EQUITY_AVAILABLE": self.handle_usdt_equity_available,
                }

                if isinstance(response_data, dict) and "action" in response_data and "request" in response_data:
                    if response_data["action"] == "GET":
                        request_type = response_data["request"]
                        handler = request_handlers.get(request_type)
                        if handler:
                            response_data = handler()
                        elif request_type == "PRICE_VALUE" \
                                and "symbol" in response_data \
                                and response_data["symbol"] in self.lst_symbols:
                            response_data = self.handle_price_value(response_data["symbol"])
                        else:
                            # Handle unknown request types if necessary
                            pass

                else:
                    # 'response_data' is either not a dictionary or doesn't contain the key 'action'
                    # Handle this case accordingly
                    pass
                # Serialize the response data to a JSON-friendly format
                response_msg = self._serialize_message(response_data)
                # Send the response back to the client
                self.socket.send_json(response_msg)
                print("Server sent reply:", response_msg)

                # Optionally, break out of loop on a certain condition (e.g., a special command)
                if isinstance(data, dict) and data.get("action") == "shutdown":
                    print("Shutdown command received. Stopping server.")
                    self.ws_client.stop()
                    break

        except KeyboardInterrupt:
            print("Server interrupted by user (KeyboardInterrupt).")
        finally:
            # Clean up ZeroMQ resources
            self.socket.close()
            self.context.term()
            print("Server socket closed and context terminated.")

    def handle_request(self, data):
        """
        Handle the incoming request data and return a response.
        This method can be extended in subclasses to implement custom logic.
        """
        # If data is a pandas DataFrame, maybe respond with its summary or shape
        if isinstance(data, pd.DataFrame):
            # Example: reply with a small description of the DataFrame
            info = {
                "type": "dataframe_info",
                "shape": data.shape,
                "columns": list(data.columns)
            }
            return info  # returning a dictionary

        # If data is a dictionary, perform some logic.
        # For example, echo the data back with an acknowledgment.
        if isinstance(data, dict):
            # Example: echo the received dict with an 'ack' field
            response_dict = data.copy()
            response_dict["ack"] = True
            return response_dict

        # If data is of any other type, just return it or a simple message
        return {"result": str(data)}

    def _serialize_message(self, data):
        """
        Convert Python data (dict or DataFrame) into a JSON-serializable message.
        The message is a dict with a 'type' and 'content'.
        """
        if isinstance(data, pd.DataFrame):
            # Convert DataFrame to dictionary in 'split' format (columns, index, data)
            content = data.to_dict(orient="split")
            msg = {"type": "dataframe", "content": content}
        elif isinstance(data, dict):
            # The data is already a dict, can be sent as JSON directly
            msg = {"type": "dict", "content": data}
        else:
            # For other types, convert to string (as a fallback)
            msg = {"type": "text", "content": str(data)}
        return msg

    def _deserialize_message(self, msg):
        """
        Convert a received JSON message back into the appropriate Python object.
        If msg is not a dict, it returns msg as is.
        The function supports two formats:
          1. A simple format with a "dataframe" key.
          2. A structured format with "type" and "content" keys.
        """
        # Check for the structured format with "type" and "content"
        mtype = msg.get("type")
        content = msg.get("content")

        # Test if msg is a dictionary
        if isinstance(msg, dict) and mtype is None and content is None:
            return msg

        if mtype == "dataframe":
            # Reconstruct DataFrame: content should have keys: 'data', 'columns', and optionally 'index'
            df = pd.DataFrame(data=content["data"], columns=content["columns"])
            if "index" in content:
                df.index = content["index"]
            return df
        elif mtype == "dict":
            return content
        else:
            # For any other type (including plain text), return the content as-is.
            return content

    def _build_df_open_positions_ws(self, df_open_positions, df_price):
        lst_open_positions_columns = ["symbol", "holdSide", "leverage", "marginCoin",
                                      "available", "total", "usdtEquity",
                                      "marketPrice", "averageOpenPrice",
                                      "achievedProfits", "unrealizedPL", "liquidationPrice"]
        if df_open_positions is None \
                or df_price is None:
            return None

        if df_price.empty \
                or df_open_positions is None \
                or df_open_positions.empty:
            return pd.DataFrame(columns=lst_open_positions_columns)

        df_open_positions = df_open_positions.copy()
        df_open_positions.rename(columns={'instId': 'symbol'}, inplace=True)
        df_open_positions.rename(columns={'openPriceAvg': 'averageOpenPrice'}, inplace=True)

        # Set the 'symbols' column as the index (if it's unique)
        df_indexed = df_price.set_index('symbols')
        for symbol in df_open_positions["symbol"].to_list():
            if symbol in df_indexed.index.to_list():
                df_open_positions["marketPrice"] = float(df_indexed.loc[symbol, 'values'])
            else:
                df_open_positions["marketPrice"] = 0
        df_open_positions["usdtEquity"] = df_open_positions["marketPrice"] * df_open_positions["total"].astype(float)

        float_columns = ["leverage", "available", "total",
                         "marketPrice", "averageOpenPrice",
                         "achievedProfits", "unrealizedPL",
                         "liquidationPrice", "totalFee"]

        for column in float_columns:
            df_open_positions[column] = df_open_positions[column].astype(float)

        df_open_positions["leverage"] = df_open_positions["leverage"].astype(int)
        df_open_positions["symbol"] = df_open_positions["symbol"] + "_UMCBL"

        for column in lst_open_positions_columns:
            if column not in df_open_positions.columns:
                print("missing column:", column)

        return df_open_positions

    def handle_triggers(self):
        return self.ws_data.get_ws_triggers()

    def handle_open_positions(self):
        return self.ws_data.get_ws_open_positions()

    def handle_open_orders(self):
        return self.ws_data.get_ws_open_orders()

    def handle_account(self):
        return self.ws_data.get_ws_account()

    def handle_prices(self):
        return self.ws_data.get_ws_prices()

    def handle_usdt_equity_available(self):
        return self.ws_data.get_usdt_equity_available()

    def handle_price_value(self, symbol):
        return self.ws_data.get_value(symbol)
