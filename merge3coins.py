#!/usr/bin/env python
# coding: utf-8
import datetime
import math
import sys
import pandas as pd

import logfile


def get_df_tick_time(df, start_index, total_rows):
    tick_time = ""
    tick_row_num = -1
    if start_index < total_rows:
        for index in range(start_index, total_rows):
            row = df.loc[index]
            row_time = getattr(row, "time")
            if row_time == "time":
                continue
            if pd.isna(getattr(row, "bs")):
                return row_time, index
    return tick_time, tick_row_num


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(f"Usage python merge3coins.py $coin1.csv $coin2.csv $coin3.csv $outputFile")
        exit(0)
    logfile.init('log')
    coin1_csv = sys.argv[1]
    coin2_csv = sys.argv[2]
    coin3_csv = sys.argv[3]
    output = sys.argv[4]
    print(f"coin1_csv: {coin1_csv} coin2_csv: {coin2_csv} coin3_csv: {coin3_csv}")

    df_coin1 = pd.read_csv(coin1_csv)
    df_coin2 = pd.read_csv(coin2_csv)
    df_coin3 = pd.read_csv(coin3_csv)

    df_coin1_index = df_coin2_index = df_coin3_index = 0
    df_coin1_write_index = df_coin2_write_index = df_coin3_write_index = 0

    df_coins = [df_coin1, df_coin2, df_coin3]
    df_start_index = [df_coin1_index, df_coin2_index, df_coin3_index]
    df_write_index = [df_coin1_write_index, df_coin2_write_index, df_coin3_write_index]
    df_rows_num = [int(df_coin1.shape[0]), int(df_coin2.shape[0]), int(df_coin3.shape[0])]
    max_rows_num = max(df_rows_num[0], df_rows_num[1], df_rows_num[2])

    # 定义时间差值表示，相差在delta内表示时间相同
    time_diff_delta = datetime.timedelta(milliseconds=5)
    print(f"time_diff_delta: {time_diff_delta}")

    # 定义之前写入的index
    previous_write_range = [[-1, 0], [-1, 0], [-1, 0]]

    result_array = []
    df_append = False

    for row_num in range(0, max_rows_num):
        # find tow times
        coin_times = []
        coin_row_indexes = []
        for index in range(0, len(df_coins)):
            coin_time, coin_row_index = get_df_tick_time(df_coins[index], df_start_index[index], df_rows_num[index])
            '''
            if len(coin_time) != 1:
                print(f"coin time len: {len(coin_time)}")
                break
            '''
            coin_times.append(coin_time)
            coin_row_indexes.append(coin_row_index)

        read_over = True
        for index in range(0, len(coin_times)):
            if coin_row_indexes[index] >= 0:
                read_over = False
                break

        if read_over:
            print(f"Can not get any row now")
            break

        # time sort
        smallest_time = coin_times[0]
        smallest_index = -1
        for index in range(0, len(coin_times)):
            if (smallest_index == -1 and coin_row_indexes[index] >= 0) or \
                    (coin_row_indexes[index] >= 0 and coin_times[index] < smallest_time):
                smallest_time = coin_times[index]
                smallest_index = index

        if smallest_index == -1:
            break

        # define writed indexes: 0: missing, 1: will write
        writed_indexes = [0, 0, 0]
        writed_indexes[smallest_index] = 1

        # 判断时间差值是否小于delta
        for index in range(0, len(coin_times)):
            if index != smallest_index and coin_row_indexes[index] >= 0:
                if pd.to_datetime(coin_times[index]) <= time_diff_delta + pd.to_datetime(coin_times[smallest_index]):
                    # print(f"datetime in delta: {pd.to_datetime(coin_times[index])}")
                    writed_indexes[index] = 1
                '''
                else:
                    print(f'pd datetime: {pd.to_datetime(coin_times[index])},'
                          f'small datatime: {pd.to_datetime(coin_times[smallest_index])}')
                '''

        # 按顺序写入币种信息
        # 未有新币种信息的币种写入上次的信息
        for index in range(0, len(writed_indexes)):
            if writed_indexes[index] == 0:
                if previous_write_range[index][0] != -1:
                    for temp in range(previous_write_range[index][0], previous_write_range[index][1]):
                        # df_output = df_output.append(df_coins[index].loc[temp])
                        result_array.append(df_coins[index].loc[temp].values)
            elif writed_indexes[index] == 1:
                for temp in range(df_start_index[index], coin_row_indexes[index] + 1):
                    # df_output = df_output.append(df_coins[index].loc[temp])
                    result_array.append(df_coins[index].loc[temp].values)
                previous_write_range[index][0] = df_start_index[index]
                previous_write_range[index][1] = coin_row_indexes[index] + 1
                df_start_index[index] = coin_row_indexes[index] + 1

        if len(result_array) >= 1000000:
            df_output = pd.DataFrame(result_array,
                                     columns=("time", "contract", "price", "bs", "amount", "last", "volume", "ask_0_p",
                                              "ask_0_v", "bid_0_p", "bid_0_v", "exchange_time", "exchange_timestamp",
                                              "timestamp", "IsDataNormal"))
            if not df_append:
                df_output.to_csv(output, index=False)
                df_append = True
                result_array = []
            else:
                df_output.to_csv(output, mode="a", index=False, header=False)
