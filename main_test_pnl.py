from src import rtdp,rtdp_simulation,broker_simulation,broker_bitget,broker_bitget_api
from datetime import datetime

params = {"exchange": "bitget", "account": "subfortest1", "reset_account": False}
my_broker = broker_bitget_api.BrokerBitGetApi(params)


product_type = 'umcbl'  # Replace with your product type
start_time = datetime(2024, 10, 29, 0, 0)  # October 29, 2024
# start_time = datetime(2024, 1, 1, 0, 0)  # October 29, 2024
end_time = datetime.now()

pnl, net_profit, num_positions, df = my_broker.get_position_history("PEPE", start_time, end_time)

print(df.to_string())
print("pnl: ", pnl)
print("net_profit: ", net_profit)
print("num_positions: ", num_positions)