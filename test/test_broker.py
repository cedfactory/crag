import pytest
import os
import csv
from src import broker,trade

class TestBrokerSimulation:

    def test_initialize(self):
        # context
        broker_simulation = broker.BrokerSimulation()
        assert(broker_simulation.get_cash() == 0)

        # action
        broker_simulation.initialize({'cash':100})

        # expectations
        assert(broker_simulation.get_cash() == 100)

    def test_get_commission(self):
        # context
        broker_simulation = broker.BrokerSimulation()

        # action
        commission = broker_simulation.get_commission("FAKE")

        # expectations
        assert(commission == 0.07)

    def generate_trade(self):
        fake_trade = trade.Trade()
        fake_trade.buying_time = ""
        fake_trade.type = "BUY"
        fake_trade.sell_id = ""
        fake_trade.stimulus= ""
        fake_trade.symbol = "ETH/USD"
        fake_trade.buying_price = 1
        fake_trade.symbol_price = 1
        fake_trade.size = 2
        fake_trade.net_price = 2
        fake_trade.buying_fee = 0.07
        fake_trade.selling_fee = 0.07
        fake_trade.roi = 0.05
        fake_trade.cash = 90
        fake_trade.portfolio_value = 110
        fake_trade.wallet_value = 110
        fake_trade.commission = fake_trade.net_price * 0.04
        fake_trade.gross_price = fake_trade.net_price + fake_trade.commission
        return fake_trade

    def test_execute_trade_ok(self):
        # context
        broker_simulation = broker.BrokerSimulation()
        broker_simulation.initialize({'cash':3})
        fake_trade = self.generate_trade()

        # action
        res = broker_simulation.execute_trade(fake_trade)

        # expectations
        assert(res == True)
        assert(len(broker_simulation.trades) == 1)

    def test_execute_trade_ko(self):
        # context
        broker_simulation = broker.BrokerSimulation()
        broker_simulation.initialize({'cash':2})
        fake_trade = self.generate_trade()

        # action
        res = broker_simulation.execute_trade(fake_trade)

        # expectations
        assert(res == False)
        assert(len(broker_simulation.trades) == 0)

    def test_export_history(self):
        # context
        broker_simulation = broker.BrokerSimulation()
        broker_simulation.initialize({'cash':3})
        fake_trade = self.generate_trade()
        broker_simulation.execute_trade(fake_trade)

        # action
        history_file_generated = "./test/generated/test_export_history.csv"
        broker_simulation.export_history(history_file_generated)

        # expectations
        lines=[ ["transaction_id", "time", "buying_time", "type", "sell_id", "stimulus", "symbol", "buying_price", "symbol_price", "size", "net_price", "buying_fees", "selling_fees", "gross_price", "roi", "remaining cash", "portfolio value", "wallet value"],
                ["BUY", "", "", "ETH/USD", "1", "1", "2", "2", "0.07", "0.07", "2.08", "0.05", "90", "110", "110"] ]
        with open(history_file_generated) as csvfile:
            csvreader = csv.reader(csvfile, delimiter=';')
            header = next(csvreader)
            assert(header == lines[0])
            
            line = next(csvreader)
            assert(line[3:] == lines[1])

        # cleaning
        os.remove(history_file_generated)