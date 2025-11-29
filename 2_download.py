import time
import requests
from datetime import datetime,timedelta
import pandas as pd

SYMBOL = "BTC_JPY"
PUBLIC_URL = "https://api.coin.z.com/public"
INTERVAL = "1min"

def get_daydf(date):
    url = f'{PUBLIC_URL}/v1/klines?symbol={SYMBOL}&interval={INTERVAL}&date={date}'
    resj = requests.get(url).json()
    if resj["status"] != 0:
        print(f"At get_hl :取得エラー\n{resj}")
        raise SystemExit(1)

    df = pd.DataFrame(resj["data"])
    df["openTime"] = pd.to_datetime(pd.to_numeric(df["openTime"]), unit="ms", errors="coerce")
    for col in ["open","high","low","close","volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.set_index("openTime", inplace=True)
    return df

def get_df(startday,endday):
    start = datetime.strptime(startday, "%Y%m%d")
    end = datetime.strptime(endday, "%Y%m%d")

    current = start
    while current <= end:
        if current == start:
            df = get_daydf(current.strftime("%Y%m%d"))
        else:
            df = pd.concat([df,get_daydf(current.strftime("%Y%m%d"))])
        current += timedelta(days=1)
        time.sleep(1)
    return df

if __name__ == "__main__":
    # Example usage
    df = get_df("20251001","20251101")
    df.to_csv("btc_jpy-1min-20251001_20251101.csv")
    print(df)