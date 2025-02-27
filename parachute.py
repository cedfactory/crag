import subprocess
import os
import sys
from src import crag_helper,broker_bitget_api
from rich import print
import platform

strategy_configuration_file = "conf_parachute_dev.xml"

configuration = crag_helper.load_configuration_file(strategy_configuration_file)

my_broker = broker_bitget_api.BrokerBitGetApi(configuration["broker"])

lst_order = []
order = {}
symbol = "BTCUSDT"
strategy_id = "PARACHUTE"

init_order = {
    "strategy_id": strategy_id,
    "symbol": symbol,
    "trigger_type": "TRIGGER",
    "type": "OPEN_LONG_ORDER",
    "TP": "",
    "SP": "",
    "triggerType": "mark_price",
    "executePrice": "0",
    "size": "1",
    "holdSide": "long",
    "parachute_gross_size": True
}

# --- Configuration Parameters ---
num_orders = 20            # Number of stop loss orders to place
start_value = 79800          # Starting reference price for the stop loss (e.g. current mark price)
decrement_percent = 0.3     # Each subsequent order is lower by this percentage (1% here)
triggerType = "mark_price"  # or "fill_price", as desired
executePrice = "0"          # Market execution
holdSide = "long"           # Use "long" for long positions (or "short" for short positions)
order_size = 0.001              # Size of the position for the stop loss

SL = False
TRIGGER = True

if SL:
    planType = "loss_plan"
    # --- Calculate and Place Stop Loss Orders ---
    for i in range(num_orders):
        # Calculate trigger price for the stop loss order.
        # For a long position, decrease the price by a percentage per order:
        stop_price = start_value * (1 - (decrement_percent / 100) * i)

        order = init_order.copy()
        order["triggerPrice"] = stop_price
        order["gross_size"] = order_size
        order["planType"] = planType
        lst_order.append(order)

        print(f"Placing stop loss order {i + 1} with trigger price: {stop_price}")

    for order in lst_order:
        my_broker.execute_open_sltp(order)

elif TRIGGER:
    planType = "normal_plan"
    for i in range(num_orders):
        # Calculate trigger price for the stop loss order.
        # For a long position, decrease the price by a percentage per order:
        stop_price = start_value * (1 - (decrement_percent / 100) * i)

        order = init_order.copy()
        order["trigger_price"] = stop_price
        order["type"] = "CLOSE_LONG_ORDER"   # CEDE IN ORDER TO SELL
        order["gross_size"] = order_size
        order["planType"] = planType
        order["side"] = "sell"
        lst_order.append(order)

        print(f"Placing stop loss order {i + 1} with trigger price: {stop_price}")

    for order in lst_order:
        my_broker.execute_trigger(order)