#!/usr/bin/env python
# coding: utf-8
import math
import sys
import gzip
import os
import pandas as pd

import util

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(f"Usage python mergetickzhubi.py $tick.csv $zhubi.csv $outputFile")
        exit(0)
    tick_csv = sys.argv[1]
    zhubi_csv = sys.argv[2]
    output = sys.argv[3]
    print(f"tick_csv: {tick_csv} zhubi_csv: {zhubi_csv}")

    df_tick = pd.read_csv(tick_csv, dtype=str,
                          names=["time", "timestamp", "last", "volume", "ask_0_p", "ask_0_v", "bid_0_p", "bid_0_v"])
    df_zhubi = pd.read_csv(zhubi_csv, dtype=str,
                           names=["exchange_time", "contract", "price", "bs", "amount", "exchange_timestamp", "time",
                                  "timestamp"])
    df_total = pd.DataFrame(columns=("exchange_time", "contract", "price", "bs", "amount", "exchange_timestamp", "time",
                                     "timestamp", "last", "volume", "ask_0_p", "ask_0_v", "bid_0_p", "bid_0_v",
                                     "IsDataNormal"))

    df_zhubi_start_index = 0
    zhubi_index = 0
    zhubi_rows_num = int(df_zhubi.shape[0])
    for tick_row in df_tick.itertuples():
        tick_time = getattr(tick_row, "time")
        if tick_time == "time":
            # df_tick.drop(index=[tick_row[0], tick_row[0]], inplace=True)
            continue
        zhubi_total_volume = 0.0
        tick_v = float(getattr(tick_row, "volume"))
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
                if tick_time < zhubi_time:
                    if not math.isclose(tick_v, zhubi_total_volume, abs_tol=0.0000001):
                        print(f"unexpected tick v: {tick_v}, total_zhubi_v: {zhubi_total_volume}")
                        # exit(0)
                    else:
                        print(f"inexpected tick v: {tick_v}, total_zhubi_v: {zhubi_total_volume}")
                        # 插入逐笔数据
                        for j in range(prev_zhubi_index, i):
                            j_data = df_zhubi.loc[j]
                            df_total = pd.concat([df_total,
                                                  pd.DataFrame({"exchange_time": [getattr(j_data, "exchange_time")],
                                                                "contract": [getattr(j_data, "contract")],
                                                                "price": [getattr(j_data, "price")],
                                                                "bs": [getattr(j_data, "bs")],
                                                                "amount": [getattr(j_data, "amount")],
                                                                "exchange_timestamp": [getattr(j_data,
                                                                                               "exchange_timestamp")],
                                                                "time": [getattr(j_data, "time")],
                                                                "timestamp": [getattr(j_data, "timestamp")],
                                                                "last": "",
                                                                "volume": "",
                                                                "ask_0_p": "",
                                                                "ask_0_v": "",
                                                                "bid_0_p": "",
                                                                "bid_0_v": "",
                                                                "IsDataNormal": "0"
                                                                })])
                        # 插入tick数据
                        df_total = pd.concat([df_total, pd.DataFrame({"exchange_time": "",
                                                                      "contract": "",
                                                                      "price": "",
                                                                      "bs": "",
                                                                      "amount": "",
                                                                      "exchange_timestamp": "",
                                                                      "time": [getattr(tick_row, "time")],
                                                                      "timestamp": getattr(tick_row, "timestamp"),
                                                                      "last": getattr(tick_row, "last"),
                                                                      "volume": getattr(tick_row, "volume"),
                                                                      "ask_0_p": getattr(tick_row, "ask_0_p"),
                                                                      "ask_0_v": getattr(tick_row, "ask_0_v"),
                                                                      "bid_0_p": getattr(tick_row, "bid_0_p"),
                                                                      "bid_0_v": getattr(tick_row, "bid_0_v"),
                                                                      "IsDataNormal": "0"
                                                                      })])
                    df_zhubi_start_index = zhubi_row[0]
                    break
                zhubi_total_volume = zhubi_total_volume + float(getattr(zhubi_row, "amount"))
    df_total.to_csv(output, index=False)
