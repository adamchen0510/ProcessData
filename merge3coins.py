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
    num_temp = 0
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

    df_coin1 = pd.read_csv(coin1_csv, dtype=str)
    df_coin2 = pd.read_csv(coin2_csv, dtype=str)
    df_coin3 = pd.read_csv(coin3_csv, dtype=str)
    df_output = pd.DataFrame(
        columns=("time", "contract", "price", "bs", "amount", "last", "volume", "ask_0_p",
                 "ask_0_v", "bid_0_p", "bid_0_v", "exchange_time", "exchange_timestamp",
                 "timestamp", "IsDataNormal"))

    df_coin1_index = df_coin2_index = df_coin3_index = 0
    df_coin1_write_index = df_coin2_write_index = df_coin3_write_index = 0

    df_coins = [df_coin1, df_coin2, df_coin3]
    df_start_index = [df_coin1_index, df_coin2_index, df_coin3_index]
    df_write_index = [df_coin1_write_index, df_coin2_write_index, df_coin3_write_index]
    df_rows_num = [int(df_coin1.shape[0]), int(df_coin2.shape[0]), int(df_coin3.shape[0])]
    max_rows_num = max(df_rows_num[0], df_rows_num[1], df_rows_num[2])

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
            if coin_row_index >= 0:
                coin_times.append(coin_time)
                coin_row_indexes.append(coin_row_index)
        if len(coin_times) == 0:
            print(f"Can not get any row now")
            break

        # time sort
        smallest_time = coin_times[0]
        smallest_index = 0
        if len(coin_times) > 1:
            for index in range(1, len(coin_times)):
                if coin_times[index] < smallest_time:
                    smallest_time = coin_times[index]
                    smallest_index = index

        for temp in range(df_start_index[smallest_index], coin_row_indexes[smallest_index] + 1):
            df_output = df_output.append(df_coins[smallest_index].loc[temp])
        df_start_index[smallest_index] = coin_row_indexes[smallest_index] + 1

        if row_num > 1000: break
    df_output.to_csv(output, index=False)
