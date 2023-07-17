from src import strategy_monitoring
import sys

if __name__ == '__main__':
    if len(sys.argv) == 1:
        strategy_monitoring.launch()
    elif len(sys.argv) >= 2:
        if sys.argv[1] == "--test":
            # launch the server
            #strategy_monitoring.launch()

            # client sends a message to the server
            #strategy_monitoring.publish_alive_strategy("account1", "strategy3")

            # rpc to the server
            client = strategy_monitoring.StrategyMonitoringClient()
            response = client.GetStrategyOnAccount("account1")
            print("response : ", response)
