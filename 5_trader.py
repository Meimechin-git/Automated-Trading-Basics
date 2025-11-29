import os
import time
import hmac
import hashlib
import json
import requests
from datetime import datetime,timedelta
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

SYMBOL = "BTC_JPY"
FOLLOW_MIN = 1
SHORT_TERM_MIN = 10
LONG_TERM_MIN = 60
LEVARAGE = 2/1.1

PUBLIC_URL = "https://api.coin.z.com/public"
PRIVATE_URL = "https://api.coin.z.com/private"

API_KEY = os.getenv("GMO_API_KEY")
API_SECRET = os.getenv("GMO_API_SECRET")

class Trader():
    def __init__(self):
        try:
            self.initial_amount_JPY = self.get_amount()
            self.current_amount_JPY = self.initial_amount_JPY
            url = f"{PUBLIC_URL}/v1/symbols"
            resj = requests.get(url).json()
            if resj["status"] != 0:
                print(f"At __init__ :取得エラー\n{resj}")
                raise SystemExit(1)
            for d in resj["data"]:
                if d["symbol"] == SYMBOL:
                    self.minOrderSize = float(d["minOrderSize"])
                    self.sizeStep = float(d["sizeStep"])

            self.normal_termination = False
            self.prot_ticker = False
        except Exception as e:
            print("例外 AT Trader/__init__:", e)
            raise SystemExit(1)



    def get_ticker(self):
        retry_interval = 3
        while True:
            try:
                url = f'{PUBLIC_URL}/v1/ticker?symbol={SYMBOL}'
                resj = requests.get(url).json()
                if resj["status"] != 0:
                    print(f"At get_hl :取得エラー\n{resj}")
                    time.sleep(retry_interval)
                    continue
                return float(resj["data"][-1]["last"])
            except Exception as e:
                print("例外 AT Trader/get_ticker:", e)
                time.sleep(retry_interval)
                continue



    def get_df(self):
        retry_interval = 3
        while True:
            try:
                date = (datetime.today()-timedelta(hours=6))
                df = pd.DataFrame(columns=["open","high","low","close","volume"])
                while len(df) < LONG_TERM_MIN:
                    url = f'{PUBLIC_URL}/v1/klines?symbol={SYMBOL}&interval=1min&date={date.strftime('%Y%m%d')}'
                    resj = requests.get(url).json()
                    if resj["status"] != 0:
                        print(f"AT Trader/get_df:取得エラー\n{resj}")
                        time.sleep(retry_interval)
                        continue
                    new_df = pd.DataFrame(resj["data"])
                    new_df["openTime"] = pd.to_datetime(pd.to_numeric(new_df["openTime"]), unit="ms", errors="coerce")
                    new_df.set_index("openTime", inplace=True)
                    if len(df) == 0:
                        df = new_df
                    else:
                        df = pd.concat([df,new_df])
                    date -= timedelta(days=1)
                for col in ["open","high","low","close","volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.tail(LONG_TERM_MIN+1)
                return df
            except Exception as e:
                print("例外 AT Trader/get_df:", e)
                time.sleep(retry_interval)
                continue



    def get_amount(self):
        retry_interval = 3
        while True:
            try:
                timestamp = '{0}000'.format(int(time.mktime(datetime.now().timetuple())))
                method = 'GET'
                path  = '/v1/account/assets'
                text = timestamp + method + path
                sign = hmac.new(bytes(API_SECRET.encode('ascii')), bytes(text.encode('ascii')), hashlib.sha256).hexdigest()

                headers = {
                    "API-KEY": API_KEY,
                    "API-TIMESTAMP": timestamp,
                    "API-SIGN": sign
                }
                resj = requests.get(PRIVATE_URL + path, headers=headers).json()
                if resj["status"] != 0:
                    print(f"At get_amount :取得エラー\n{resj}")
                    time.sleep(retry_interval)
                    continue
                amount = 0
                for data in resj["data"]:
                    if data["symbol"] == "JPY":
                        amount = float(data["amount"])
                return amount
            except Exception as e:
                print("例外 AT Trader/get_amount:", e)
                time.sleep(retry_interval)
                continue



    def get_position(self):
        retry_interval = 3
        while True:
            try:
                timestamp = '{0}000'.format(int(time.mktime(datetime.now().timetuple())))
                method    = 'GET'
                path      = '/v1/positionSummary'

                text = timestamp + method + path
                sign = hmac.new(bytes(API_SECRET.encode('ascii')), bytes(text.encode('ascii')), hashlib.sha256).hexdigest()
                headers = {
                    "API-KEY": API_KEY,
                    "API-TIMESTAMP": timestamp,
                    "API-SIGN": sign
                }
                resj = requests.get(PRIVATE_URL + path, headers=headers).json()
                if resj["status"] != 0:
                    print(f"At get_position :取得エラー\n{resj}")
                    time.sleep(retry_interval)
                    continue
                side = ""
                amount = 0
                loss_gain = 0
                for data in resj["data"]["list"]:
                    if data["symbol"] == SYMBOL:
                        side = data["side"]
                        amount = float(data["sumPositionQuantity"])
                        loss_gain = float(data["positionLossGain"])
                return side,amount,loss_gain
            except Exception as e:
                time.sleep(retry_interval)
                continue



    def open_stop_order(self,price,side):
        retry_interval = 3
        while True:
            try:
                amount = self.current_amount_JPY/price*LEVARAGE
                amount = round((amount-amount%self.sizeStep),6) #マジックナンバーがついているので注意！
                if amount >= self.minOrderSize:
                    timestamp = '{0}000'.format(int(time.mktime(datetime.now().timetuple())))
                    method    = 'POST'
                    path      = '/v1/order'
                    reqBody = {
                        "symbol": SYMBOL,
                        "side": side,
                        "price": f"{int(price)}",
                        "executionType": "STOP",
                        "size": f"{amount}"
                    }
                    text = timestamp + method + path + json.dumps(reqBody)
                    sign = hmac.new(bytes(API_SECRET.encode('ascii')), bytes(text.encode('ascii')), hashlib.sha256).hexdigest()

                    headers = {
                        "API-KEY": API_KEY,
                        "API-TIMESTAMP": timestamp,
                        "API-SIGN": sign
                    }
                    resj = requests.post(PRIVATE_URL + path, headers=headers, data=json.dumps(reqBody)).json()
                    if resj["status"] != 0:
                        return -1
                else :
                    print("最小注文量を下回っています。")
                    raise SystemExit(1)
                print(f"{datetime.today()}: {price} PRICEで{side}で{amount} {SYMBOL}の建玉を新規STOP注文しました")
                return int(resj["data"])
            except Exception as e:
                print("例外 AT Trader/open_stop_order:", e)
                time.sleep(retry_interval)
                continue



    def close_stop_order(self,price):
        retry_interval = 3
        while True:
            try:
                position = self.get_position()
                side = "BUY" if position[0] == "SELL" else "SELL"
                amount = position[1]
                timestamp = '{0}000'.format(int(time.mktime(datetime.now().timetuple())))
                method    = 'POST'
                path      = '/v1/closeBulkOrder'
                reqBody = {
                    "symbol": SYMBOL,
                    "side": side,
                    "executionType": "STOP",
                    "price": f"{int(price)}",
                    "size": f"{amount}"
                }
                text = timestamp + method + path + json.dumps(reqBody)
                sign = hmac.new(bytes(API_SECRET.encode('ascii')), bytes(text.encode('ascii')), hashlib.sha256).hexdigest()

                headers = {
                    "API-KEY": API_KEY,
                    "API-TIMESTAMP": timestamp,
                    "API-SIGN": sign
                }
                resj = (requests.post(PRIVATE_URL + path, headers=headers, data=json.dumps(reqBody))).json()
                if resj["status"] != 0:
                    print(f"At Trader/close_stop_order :取得エラー\n{resj}")
                    self.close_market_order()
                    return -1
                print(f"{datetime.today()}: {price} PRICEで{side}で{amount} {SYMBOL}の建玉を決済STOP注文しました")
                return int(resj["data"])
            except Exception as e:
                print("例外 AT Trader/close_stop_order:", e)
                time.sleep(retry_interval)
                continue



    def close_market_order(self):
        retry_interval = 3
        while True:
            try:
                position = self.get_position()
                if position[0] == "":
                    print("close_market_orderが呼び出されましたが、建玉がありませんでした。")
                    return 0
                side = "BUY" if position[0] == "SELL" else "SELL"
                amount = position[1]
                timestamp = '{0}000'.format(int(time.mktime(datetime.now().timetuple())))
                method    = 'POST'
                path      = '/v1/closeBulkOrder'
                reqBody = {
                    "symbol": SYMBOL,
                    "side": side,
                    "executionType": "MARKET",
                    "size": f"{amount}"
                }
                text = timestamp + method + path + json.dumps(reqBody)
                sign = hmac.new(bytes(API_SECRET.encode('ascii')), bytes(text.encode('ascii')), hashlib.sha256).hexdigest()

                headers = {
                    "API-KEY": API_KEY,
                    "API-TIMESTAMP": timestamp,
                    "API-SIGN": sign
                }
                resj = (requests.post(PRIVATE_URL + path, headers=headers, data=json.dumps(reqBody))).json()
                if resj["status"] != 0:
                    print(f"At Trader/close_market_order :取得エラー\n{resj}")
                    time.sleep(retry_interval)
                    continue
                while self.get_position()[1] != 0:
                    time.sleep(0.5)
                print(f"{datetime.today()}: {self.get_ticker()} PRICEで建玉売却を確定しました")
                return 0
            except Exception as e:
                print("例外 AT Trader/close_market_order:", e)
                time.sleep(retry_interval)
                continue



    def change_stop_oder(self,id,price):
        retry_interval = 3
        while True:
            try:
                timestamp = '{0}000'.format(int(time.mktime(datetime.now().timetuple())))
                method    = 'POST'
                path      = '/v1/changeOrder'
                reqBody = {
                        "orderId": id,
                        "price": f"{int(price)}",
                }
                text = timestamp + method + path + json.dumps(reqBody)
                sign = hmac.new(bytes(API_SECRET.encode('ascii')), bytes(text.encode('ascii')), hashlib.sha256).hexdigest()

                headers = {
                    "API-KEY": API_KEY,
                    "API-TIMESTAMP": timestamp,
                    "API-SIGN": sign
                }
                resj = (requests.post(PRIVATE_URL + path, headers=headers, data=json.dumps(reqBody))).json()
                if resj["status"] != 0:
                    print("決済STOP注文の変更をキャンセルしました")
                    return id
                print(f"{datetime.today()}: {price} PRICEに決済STOP注文を変更しました")
                return id
            except Exception as e:
                print("例外 AT Trader/close_stop_change_oder:", e)
                time.sleep(retry_interval)
                continue



    def cancel_oder(self,id):
        retry_interval = 3
        while True:
            try:
                timestamp = '{0}000'.format(int(time.mktime(datetime.now().timetuple())))
                method    = 'POST'
                path      = '/v1/cancelOrder'
                reqBody = {
                        "orderId": id,
                }
                text = timestamp + method + path + json.dumps(reqBody)
                sign = hmac.new(bytes(API_SECRET.encode('ascii')), bytes(text.encode('ascii')), hashlib.sha256).hexdigest()

                headers = {
                    "API-KEY": API_KEY,
                    "API-TIMESTAMP": timestamp,
                    "API-SIGN": sign
                }
                resj = (requests.post(PRIVATE_URL + path, headers=headers, data=json.dumps(reqBody))).json()
                if resj["status"] != 0:
                    return -1
                print(f"{datetime.today()}: 決済STOP注文をキャンセルしました")
                return -1
            except Exception as e:
                print("例外 AT Trader/close_stop_change_oder:", e)
                time.sleep(retry_interval)
                continue