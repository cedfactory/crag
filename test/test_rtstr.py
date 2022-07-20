from . import test_rtctrl

def update_rtctrl(rtstr):
    current_trades = test_rtctrl.get_current_trades_sample()

    # action
    prices_symbols = {'symbol1': 0.01, 'symbol2': 0.02, 'symbol3': 0.03, 'symbol4': 0.04}
    current_datetime = "2022-04-01"
    rtstr.rtctrl.update_rtctrl(current_datetime, current_trades, 100, prices_symbols)

    return rtstr
