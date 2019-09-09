#!/usr/bin/env python
# coding: utf-8
import datetime
import math
import sys
import pandas as pd

import logfile


def new_zhubi_df(df_row, normal):
    return pd.DataFrame({"time": [pd.to_datetime(getattr(df_row, "time"))],
                         "contract": [getattr(df_row, "contract")],
                         "price": [getattr(df_row, "price")],
                         "bs": [getattr(df_row, "bs")],
                         "amount": [getattr(df_row, "amount")],
                         "last": "",
                         "volume": "",
                         "ask_0_p": "",
                         "ask_0_v": "",
                         "bid_0_p": "",
                         "bid_0_v": "",
                         "exchange_time": [getattr(df_row, "exchange_time")],
                         "exchange_timestamp": [getattr(df_row,
                                                        "exchange_timestamp")],
                         "timestamp": [getattr(df_row, "timestamp")],
                         "IsDataNormal": normal
                         })


def new_tick_df(df_row, contract_name):
    return pd.DataFrame({"time": [pd.to_datetime(getattr(df_row, "time"))],
                         "contract": contract_name,
                         "price": "",
                         "bs": "",
                         "amount": "",
                         "last": getattr(df_row, "last"),
                         "volume": getattr(df_row, "volume"),
                         "ask_0_p": getattr(df_row, "ask_0_p"),
                         "ask_0_v": getattr(df_row, "ask_0_v"),
                         "bid_0_p": getattr(df_row, "bid_0_p"),
                         "bid_0_v": getattr(df_row, "bid_0_v"),
                         "exchange_time": "",
                         "exchange_timestamp": "",
                         "timestamp": getattr(df_row, "timestamp"),
                         "IsDataNormal": 1
                         })




if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(f"Usage python mergetickzhubi.py $tick.csv $zhubi.csv $outputFile")
        exit(0)
    logfile.init('log')
    tick_csv = sys.argv[1]
    zhubi_csv = sys.argv[2]
    output = sys.argv[3]
    print(f"tick_csv: {tick_csv} zhubi_csv: {zhubi_csv}")

    df_tick = pd.read_csv(tick_csv)
    df_zhubi = pd.read_csv(zhubi_csv)
    df_total = pd.DataFrame(columns=("time", "contract", "price", "bs", "amount", "last", "volume", "ask_0_p",
                                     "ask_0_v", "bid_0_p", "bid_0_v", "exchange_time", "exchange_timestamp",
                                     "timestamp", "IsDataNormal"))

    df_tick.info()
    df_zhubi.info()

    zhubi_index = 0
    zhubi_rows_num = int(df_zhubi.shape[0])
    contract = ""

    # 定义时间差值，相差在delta内算作同一个tick内
    time_diff_delta = datetime.timedelta(milliseconds=500)
    total_search_num = 1
    for tick_row in df_tick.itertuples():
        #logfile.debug(str.format("tick_row index {}", tick_row[0]))
        tick_time = getattr(tick_row, "time")
        if tick_time == "time":
            # df_tick.drop(index=[tick_row[0], tick_row[0]], inplace=True)
            continue
        zhubi_total_volume = 0.0
        tick_v = getattr(tick_row, "volume")
        if tick_v > 0.0:
            tick_time = getattr(tick_row, "time")
            prev_zhubi_index = zhubi_index
            # for zhubi_row in df_zhubi.itertuples():
            for i in range(zhubi_index, zhubi_rows_num):
                zhubi_index = i
                zhubi_row = df_zhubi.loc[i]
                zhubi_time = getattr(zhubi_row, "time")
                if zhubi_time == "time":
                    prev_zhubi_index = prev_zhubi_index + 1
                    continue
                #logfile.debug(str.format("zhubi_row index {}", i))
                contract = getattr(zhubi_row, "contract")
                if tick_time < zhubi_time:
                    if math.isclose(tick_v, zhubi_total_volume, abs_tol=0.0000001):
                        # print(f"inexpected tick v: {tick_v}, total_zhubi_v: {zhubi_total_volume}")
                        # 插入逐笔数据
                        for j in range(prev_zhubi_index, i):
                            j_data = df_zhubi.loc[j]
                            df_total = pd.concat([df_total, new_zhubi_df(j_data, 1)])
                        # 插入tick数据
                        df_total = pd.concat([df_total, new_tick_df(tick_row, contract)])
                    else:
                        '''
                        if tick_v > zhubi_total_volume:
                            print(f"tick_v bigger than zhubi_total_volume")
                            temp_zhubi_total_volume = zhubi_total_volume
                            for k in range(0, total_search_num):
                                temp_row = df_zhubi.loc[i+1+k]
                                temp_time = getattr(temp_row, "time")
                                if pd.to_datetime(temp_time) <= time_diff_delta + pd.to_datetime(tick_time):
                                    temp_zhubi_total_volume = temp_zhubi_total_volume + getattr(temp_row, "amount")
                                    if math.isclose(tick_v, temp_zhubi_total_volume, abs_tol=0.0000001):
                                        # 插入逐笔数据
                                        print(f"Insert 0")
                                        for j in range(prev_zhubi_index, i+k+1):
                                            j_data = df_zhubi.loc[j]
                                            df_total = pd.concat([df_total, new_zhubi_df(j_data, 0)])
                                        # 插入tick数据
                                        df_total = pd.concat([df_total, new_tick_df(tick_row, contract)])
                                    else:
                                        print(f"unexpected tick v: {tick_v}, total_zhubi_v: {zhubi_total_volume}")
                                else:
                                    print(f"temp_time: {pd.to_datetime(temp_time)} tick_time: {pd.to_datetime(tick_time)}")
                        else:
                            print(f"Error: tick v: {tick_v} volume smaller than total zhubi volume: {zhubi_total_volume}")
                        '''
                        # print(f"unexpected tick v: {tick_v}, total_zhubi_v: {zhubi_total_volume}")
                    break
                zhubi_total_volume = zhubi_total_volume + getattr(zhubi_row, "amount")
        if tick_row[0] > 1000: break
    df_total.to_csv(output, index=False)
