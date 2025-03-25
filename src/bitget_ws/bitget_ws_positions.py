import bitget_ws_client
import json

class WSPositions:

    def __init__(self, params = None):
        self.client = None
        self.id = None

        if not params:
            return

        self.id = params.get("id", self.id)
        api_key = params.get("api_key", None)
        api_secret = params.get("api_secret", None)
        api_passphrase = params.get("api_passphrase", None)
        if not api_key or not api_secret or not api_passphrase:
            return

        self.client = bitget_ws_client.BitgetWsClient(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=api_passphrase,
            ws_url=bitget_ws_client.CONTRACT_WS_URL_PRIVATE,
            verbose=True) \
            .error_listener(bitget_ws_client.handel_error) \
            .build()

        channels = [bitget_ws_client.SubscribeReqCoin("USDT-FUTURES", "account", "default")]

        self.marginCoin = None
        self.frozen = 0
        self.available = 0
        self.maxOpenPosAvailable = 0
        self.maxTransferOut = 0
        self.equity = 0
        self.usdtEquity = 0

        def on_message_equity(message):
            try:
                json_obj = json.loads(message)
                data = json_obj["data"][0]
                '''self.marginCoin = data.get("marginCoin", None)
                self.frozen = float(data["frozen"])
                self.available = float(data["available"])
                self.maxOpenPosAvailable = float(data["maxOpenPosAvailable"])
                self.maxTransferOut = float(data["maxTransferOut"])
                self.equity = float(data["equity"])'''
                self.usdtEquity = float(data["usdtEquity"])
                return
            except Exception as ex:
                print(ex)

        self.client.subscribe(channels, on_message_equity)

    def stop(self):
        self.client.close()

    def __del__(self):
        print("destructor")

    def dump(self):
        print("marginCoin : ", self.marginCoin)
        print("frozen : ", self.frozen)
        print("available : ", self.available)
        print("maxOpenPosAvailable : ", self.maxOpenPosAvailable)
        print("maxTransferOut : ", self.maxTransferOut)
        print("equity : ", self.equity)
        print("usdtEquity : ", self.usdtEquity)

    def can_request(self):
        return True

    def request(self, service, params=None):
        if service == "usdt_equity":
            return self.usdtEquity
        return 0

