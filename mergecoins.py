#!/usr/bin/env python
# coding: utf-8
import datetime
import math
import sys
import pandas as pd

import logfile


def get_df_tick_time(df, start_index, total_rows, num_time):
    tick_time = []
    tick_row_num = []
    num_temp = 0
    for index in range(start_index, total_rows):
        row = df.loc[index]
        row_time = getattr(row, "time")
        if row_time == "time":
            continue
        if pd.isna(getattr(row, "bs")):
            tick_time.append(row_time)
            tick_row_num.append(index)
            num_temp = num_temp + 1
            if num_temp >= num_time:
                break
    return tick_time, tick_row_num


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(f"Usage python mergecoins.py $coin1.csv $coin2.csv  $outputFile")
        exit(0)
    logfile.init('log')
    eth_csv = sys.argv[1]
    lrc_csv = sys.argv[2]
    output = sys.argv[3]
    print(f"eth_csv: {eth_csv} lrc_csv: {lrc_csv}")

    eth_symbol = "eth.usdt"
    lrc_symbol = "lrc.eth"

    df_eth = pd.read_csv(eth_csv, dtype=str)
    df_lrc = pd.read_csv(lrc_csv, dtype=str)
    df_output = pd.DataFrame(
        columns=("time", "contract", "price", "bs", "amount", "last", "volume", "ask_0_p",
                 "ask_0_v", "bid_0_p", "bid_0_v", "exchange_time", "exchange_timestamp",
                 "timestamp", "IsDataNormal"))

    df_eth_write_index = 0
    df_lrc_write_index = 0
    df_lrc_start_index = 0  # hard code to 1
    df_eth_start_index = 0  # hard code to 1
    lrc_rows_num = int(df_lrc.shape[0])
    eth_rows_num = int(df_eth.shape[0])
    logfile.debug(str.format("eth_row_num: {} lrc_row_num: {}", eth_rows_num, lrc_rows_num))
    # for eth_row in df_eth.itertuples():
    max_rows_num = max(lrc_rows_num, eth_rows_num)

    for indexes in range(df_eth_start_index, max_rows_num):
        eth_times, eth_row_index = get_df_tick_time(df_eth, df_eth_start_index, eth_rows_num, 1)
        lrc_times, lrc_row_index = get_df_tick_time(df_lrc, df_lrc_start_index, lrc_rows_num, 1)

        if len(eth_times) != 1 or len(lrc_times) != 1:
            print(f'eth_times: {eth_times}, size: {len(eth_times)}')
            print(f'lrc_times: {lrc_times}, size: {len(lrc_times)}')
            break

        if eth_times[0] >= lrc_times[0]:
            for temp in range(df_lrc_start_index, lrc_row_index[0] + 1):
                df_output = df_output.append(df_lrc.loc[temp])
            df_lrc_start_index = lrc_row_index[0] + 1
            '''
            if eth_times[0] < lrc_times[1]:
                for temp in range(df_eth_start_index, eth_row_index[0] + 1):
                    df_output = df_output.append(df_eth.loc[temp])
                df_eth_start_index = eth_row_index[0] + 1
            '''
        else:
            for temp in range(df_eth_start_index,  eth_row_index[0] + 1):
                df_output = df_output.append(df_eth.loc[temp])
            df_eth_start_index = eth_row_index[0] + 1
            '''
            if eth_times[1] < lrc_times[0]:
                for temp in range(df_eth_start_index, eth_row_index[1] + 1):
                    df_output = df_output.append(df_eth.loc[temp])
                df_eth_start_index = eth_row_index[1] + 1
            '''

        if eth_row_index[0] > 1000: break
    df_output.to_csv(output, index=False)
