#!/usr/bin/env python
# coding: utf-8
import datetime
import math
import sys
import pandas as pd

import logfile


def new_zhubi_df(df_row, normal):
    return [pd.to_datetime(getattr(df_row, "time")),  # time
            getattr(df_row, "contract"),  # "contract":
            getattr(df_row, "price"),  # " price":
            getattr(df_row, "bs"),  # "bs":
            getattr(df_row, "amount"),  # "amount":
            "",  # "last":
            "",  # "volume":
            "",  # "ask_0_p":
            "",  # "ask_0_v":
            "",  # "bid_0_p":
            "",  # "bid_0_v":
            getattr(df_row, "exchange_time"),  # "exchange_time":
            getattr(df_row, "exchange_timestamp"),  # "exchange_timestamp":
            getattr(df_row, "timestamp"),  # "timestamp":
            normal  # "IsDataNormal":
            ]


def new_tick_df(df_row, contract_name):
    return [pd.to_datetime(getattr(df_row, "time")),  # time
            contract_name,  # "contract":
            "",  # " price":
            "",  # "bs":
            "",  # "amount":
            getattr(df_row, "last"),  # "last":
            getattr(df_row, "volume"),  # "volume":
            getattr(df_row, "ask_0_p"),  # "ask_0_p":
            getattr(df_row, "ask_0_v"),  # "ask_0_v":
            getattr(df_row, "bid_0_p"),  # "bid_0_p":
            getattr(df_row, "bid_0_v"),  # "bid_0_v":
            "",  # "exchange_time":
            "",  # "exchange_timestamp":
            getattr(df_row, "timestamp"),  # "timestamp":
            1  # "IsDataNormal":
            ]


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(f"Usage python mergetickzhubi.py $tick.csv $zhubi.csv $outputFile")
        exit(0)
    logfile.init('log')
    tick_csv = sys.argv[1]
    zhubi_csv = sys.argv[2]
    output = sys.argv[3]
    print(f"tick_csv: {tick_csv} zhubi_csv: {zhubi_csv}")

    df_tick = pd.read_csv(tick_csv, float_precision='round_trip')
    df_zhubi = pd.read_csv(zhubi_csv, float_precision='round_trip')
    '''
    df_total = pd.DataFrame(columns=("time", "contract", "price", "bs", "amount", "last", "volume", "ask_0_p",
                                     "ask_0_v", "bid_0_p", "bid_0_v", "exchange_time", "exchange_timestamp",
                                     "timestamp", "IsDataNormal"))
    '''

    df_tick.info()
    df_zhubi.info()

    zhubi_index = 0
    zhubi_rows_num = int(df_zhubi.shape[0])
    contract = ""

    # 定义时间差值，相差在delta内算作同一个tick内
    time_diff_delta = datetime.timedelta(milliseconds=50)
    total_search_num = 1
    prev_total_fail = False
    prev_zhubi_volume = 0.0

    result_array = []
    for tick_row in df_tick.itertuples():
        # logfile.debug(str.format("tick_row index {}", tick_row[0]))
        tick_time = getattr(tick_row, 'time')
        if tick_time == "time":
            # df_tick.drop(index=[tick_row[0], tick_row[0]], inplace=True)
            continue
        zhubi_total_volume = 0.0
        if prev_total_fail:
            zhubi_total_volume = prev_zhubi_volume
            prev_total_fail = False
            zhubi_index = zhubi_index - 1
        tick_v = getattr(tick_row, 'volume')
        if tick_v > 0.0:
            prev_zhubi_index = zhubi_index
            # for zhubi_row in df_zhubi.itertuples():
            for i in range(zhubi_index, zhubi_rows_num):
                zhubi_index = i
                zhubi_row = df_zhubi.loc[i]
                zhubi_time = getattr(zhubi_row, "time")
                if zhubi_time == "time":
                    prev_zhubi_index = prev_zhubi_index + 1
                    continue
                # logfile.debug(str.format("zhubi_row index {}", i))
                contract = getattr(zhubi_row, "contract")
                if math.isclose(tick_v, zhubi_total_volume, abs_tol=0.0000001):
                    # if tick_time >= zhubi_time:
                    print(f"Expected tick v: {tick_v}, total_zhubi_v: {zhubi_total_volume}")
                    # 插入逐笔数据
                    for j in range(prev_zhubi_index, i):
                        j_data = df_zhubi.loc[j]
                        temp_time = getattr(j_data, "time")
                        if temp_time >= tick_time:
                            print(f"Abnormal time: row: {j_data[0]} zhubi_time: {temp_time}, tick_time: {tick_time}")
                            result_array.append(new_zhubi_df(j_data, 0))
                        else:
                            result_array.append(new_zhubi_df(j_data, 1))
                    # 插入tick数据
                    result_array.append(new_tick_df(tick_row, contract))
                    break
                # 逐笔time大于tick_time+diff
                else:
                    if zhubi_time > tick_time:
                        if pd.to_datetime(zhubi_time) > time_diff_delta + pd.to_datetime(tick_time):
                            # or pd.to_datetime(tick_time) > time_diff_delta + pd.to_datetime(zhubi_time):
                            print(f"Ignore Invalid time, zhubi_total_volume: {zhubi_total_volume}, tick_v: {tick_v}")
                            break
                    # print(f"unexpected tick v: {tick_v}, total_zhubi_v: {zhubi_total_volume}")
                prev_zhubi_volume = getattr(zhubi_row, "amount")
                zhubi_total_volume = zhubi_total_volume + prev_zhubi_volume
        #if tick_row[0] > 1000: break
    df_total = pd.DataFrame(result_array,
                            columns=("time", "contract", "price", "bs", "amount", "last", "volume", "ask_0_p",
                                     "ask_0_v", "bid_0_p", "bid_0_v", "exchange_time", "exchange_timestamp",
                                     "timestamp", "IsDataNormal"))
    df_total.to_csv(output, index=False, )
