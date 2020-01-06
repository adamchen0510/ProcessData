#!/usr/bin/env python
# coding: utf-8
import datetime
import sys
import gzip
import os
import pandas as pd
import util

if __name__ == "__main__":
    if (len(sys.argv) < 3):
        print(f"Usage python cleantick.py $inputDir $outputFile")
        exit(0)
    data_dir = sys.argv[1]
    output = sys.argv[2]
    print(f"data dir is {data_dir}")

    file_list = os.listdir(data_dir)
    file_list.sort()
    df = pd.DataFrame(columns=("time", "timestamp", "last", "volume", "ask_0_p", "ask_0_v", "bid_0_p", "bid_0_v"))

    tick_diff_time = datetime.timedelta(seconds=150)

    prev_price = 0.0
    prev_time = ''
    prev_set = False
    for i in range(0, len(file_list)):
        path = os.path.join(data_dir, file_list[i])
        if util.is_tick_raw_file(path):
            print(f"file is:{path}")
            df_temp = pd.read_csv(path)
            # ,names=["time", "timestamp", "last", "volume", "ask_0_p", "ask_0_v", "bid_0_p", "bid_0_v"])

            df_temp.info()
            # 删除有空值的行
            df_temp = df_temp.dropna()

            df = pd.concat([df, df_temp])
            for row in df.itertuples():
                row_time = getattr(row, "time")
                if row_time == "time":
                    df.drop(index=[row[0], row[0]], inplace=True)
                    continue
                ask_price = getattr(row, "ask_0_p")
                bid_price = getattr(row, "bid_0_p")
                ask_volume = getattr(row, "ask_0_v")
                bid_volume = getattr(row, "bid_0_v")
                if ask_price == 0.0 or bid_price == 0.0 or ask_volume == 0.0 or bid_volume == 0.0:
                    print(f"Invalid price/volume {ask_price} {ask_volume} {bid_price} {bid_volume}")
                    df.drop(index=[row[0], row[0]], inplace=True)
                    continue

                # check price and volume
                if ask_price <= 0.0 or \
                        bid_price <= 0.0 or \
                        ask_volume <= 0.0 or \
                        bid_volume <= 0.0:
                    print(f"unexpected ask_price: {ask_price}, bid_price: {bid_price}, ask_volume: {ask_volume}, "
                          f"bid_volume: {bid_volume}, row: {row[0]}")
                    df.drop(index=[row[0], row[0]], inplace=True)
                elif ask_price <= bid_price:
                    print(f"Exception, ask_price: {ask_price} <= bid_price: {bid_price}, row: {row[0]}")
                    df.drop(index=[row[0], row[0]], inplace=True)
                #elif prev_set and pd.to_datetime(prev_time) + tick_diff_time < pd.to_datetime(getattr(row, "time")):
                #    print(f"Invalid time, current: {getattr(row, 'time')}, prev: {prev_time}")
                #    df.drop(index=[row[0], row[0]], inplace=True)
                #    continue
                elif prev_set and (ask_price / prev_price > 1.2 or ask_price / prev_price < 0.8):
                    print(f"Invalid price, current: {ask_price} prev: {prev_price}")
                    df.drop(index=[row[0], row[0]], inplace=True)
                    continue

                prev_time = getattr(row, "time")
                prev_price = ask_price
                prev_set = True

    df.to_csv(output, index=False)
