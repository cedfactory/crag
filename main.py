from src import rtdp_tv

if __name__ == '__main__':
    print('CRAG')

    rtdp_tv = rtdp_tv.RTDPTradingView()
    selection = rtdp_tv.next()
    print(selection)
