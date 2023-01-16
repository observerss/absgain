import json
import os
import pickle
import sys
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
import tushare as ts

assert sys.platform == "linux"


def select_symbols():
    # http://fund.eastmoney.com/data/fundranking.html#thh;c0;r;sdm;pn10000;dasc;qsd20200924;qed20210924;qdii;zq;gg;gzbd;gzfs;bbzt;sfbb
    # inspect data
    rows = json.loads(open("funds.data", "rb").read()[22:-162])
    columns = [
        "symbol",
        "name",
        "brief",
        "last_traded_at",
        "net_value",
        "all_time_net_value",
        "1day_return",
        "7day_return",
        "30d_return",
        "90d_return",
        "180d_return",
        "1y_return",
        "2y_return",
        "3y_return",
        "this_year_return",
        "all_time_return",
        "issued_at",
        "min_subscription",
        "custom",
        "subscription_fee",
        "subscription_fee_discounted",
        "min_kept_subscription",
        "x1",
        "x2",
        "amount",
    ]
    df = pd.DataFrame(map(lambda x: x.split(","), rows), columns=columns)
    df = df[df.name.str.endswith("C")][df.issued_at <= "2018"]
    df = df[df.last_traded_at.str.startswith("2023")]
    print(df)
    open("symbols.txt", "w").write("\n".join(df.symbol.to_list()))


def get_fund_k_history(fund_code: str, pz: int = 40000) -> pd.DataFrame:
    """
    根据基金代码和要获取的页码抓取基金净值信息

    Parameters
    ----------
    fund_code : 6位基金代码
    page : 页码 1 为最新页数据

    Return
    ------
    DataFrame : 包含基金历史k线数据
    """
    # 请求头
    EastmoneyFundHeaders = {
        "User-Agent": "EMProjJijin/6.2.8 (iPhone; iOS 13.6; Scale/2.00)",
        "GTOKEN": "98B423068C1F4DEF9842F82ADF08C5db",
        "clientInfo": "ttjj-iPhone10,1-iOS-iOS13.6",
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "fundmobapi.eastmoney.com",
        "Referer": "https://mpservice.com/516939c37bdb4ba2b1138c50cf69a2e1/release/pages/FundHistoryNetWorth",
    }
    # 请求参数
    data = {
        "FCODE": f"{fund_code}",
        "appType": "ttjj",
        "cToken": "1",
        "deviceid": "1",
        "pageIndex": "1",
        "pageSize": f"{pz}",
        "plat": "Iphone",
        "product": "EFund",
        "serverVersion": "6.2.8",
        "version": "6.2.8",
    }
    url = "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNHisNetList"
    json_response = requests.get(url, headers=EastmoneyFundHeaders, data=data).json()
    rows = []
    columns = ["日期", "单位净值", "累计净值", "涨跌幅"]
    if json_response is None:
        return pd.DataFrame(rows, columns=columns)
    datas = json_response["Datas"]
    if len(datas) == 0:
        return pd.DataFrame(rows, columns=columns)
    rows = []
    for stock in datas:
        date = stock["FSRQ"]
        rows.append(
            {
                "日期": date,
                "单位净值": stock["DWJZ"],
                "累计净值": stock["LJJZ"],
                "涨跌幅": stock["JZZZL"],
            }
        )

    df = pd.DataFrame(rows)
    df["单位净值"] = pd.to_numeric(df["单位净值"], errors="coerce")

    df["累计净值"] = pd.to_numeric(df["累计净值"], errors="coerce")

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    return df


def get_all_symbols(recent_days=14):
    for symbol in open("symbols.txt").read().splitlines():
        symbol = symbol.strip()
        filename = f"symbols/{symbol}.csv"
        if os.path.exists(filename):
            last_date = datetime.strptime(
                open(filename).readlines()[1].split(",")[1], "%Y-%m-%d"
            )
            recent_date = datetime.now() - timedelta(days=recent_days)
            print(symbol, last_date, recent_date)
            if last_date < recent_date:
                df = get_fund_k_history(symbol)
                df.to_csv(filename)
                time.sleep(1)
        else:
            print("k history", symbol)
            df = get_fund_k_history(symbol)
            df.to_csv(filename)
            time.sleep(1)


def make_dfs(min_hist=500, min_size=3):
    try:
        dfs = pickle.load(open("dfs.pkl", "rb"))
    except:
        dfs = {}
    print(dfs.keys())
    symbols = open("symbols.txt").read().split()
    for symbol in symbols:
        if (symbol not in dfs) or dfs[symbol] is None:
            print("fund info", symbol)
            try:
                df = ts.get_fund_info(symbol)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print('error', str(e))
                continue
            if df.jjgm.values[0] < min_size:
                df = None
            else:
                df = pd.read_csv(f"symbols/{symbol}.csv")
                if df.shape[0] < min_hist:
                    df = None
                else:
                    df.涨跌幅 = df.涨跌幅.str.replace("--", "0")
                    a = np.array(df.涨跌幅.values, dtype=np.float64)
                    sharpe = a.mean() / a.std() * np.sqrt(252)
                    if sharpe < 1:
                        # df = None
                        pass
            dfs[symbol] = df
            time.sleep(1)
            pickle.dump(dfs, open("dfs.pkl", "wb"))


def print_small_drawdowns():
    dfs = pickle.load(open("dfs.pkl", "rb"))
    for symbol, df in dfs.items():
        if df is not None:
            xs = df.累计净值.values[::-1]
            i = np.argmax(np.maximum.accumulate(xs) - xs)  # end of maxdd
            j = np.argmax(xs[:i])  # start of maxdd
            maxdd = xs[j] - xs[i]
            maxdd_period = i - j
            if maxdd / xs[i] < 0.05 and maxdd_period < 40:
                print(symbol, maxdd, maxdd_period)


if __name__ == "__main__":
    # select_symbols()
    # get_all_symbols()
    make_dfs(min_hist=800, min_size=3)  # 最少800天交易, 最小3亿规模
