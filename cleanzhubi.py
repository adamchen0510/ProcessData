#!/usr/bin/env python
# coding: utf-8

import sys
import gzip
import os
import pandas as pd
import util

if __name__ == "__main__":
    if (len(sys.argv) < 3):
        print(f"Usage python cleanzhubi.py $inputDir $outputFile")
        exit(0)
    data_dir = sys.argv[1]
    output = sys.argv[2]
    print(f"data dir is {data_dir}")

    file_list = os.listdir(data_dir)
    file_list.sort()
    df = pd.DataFrame(columns=("exchange_time", "contract", "price", "bs", "amount", "exchange_timestamp", "time", "timestamp"))
    for i in range(0, len(file_list)):
        path = os.path.join(data_dir, file_list[i])
        if util.is_zhubi_raw_file(path):
            print(f"file is:{path}")
            df_temp = pd.read_csv(path, names=["exchange_time", "contract", "price", "bs", "amount", "exchange_timestamp", "time", "timestamp"])

            # 删除有空值的行
            df_temp = df_temp.dropna()

            df = pd.concat([df, df_temp])
            for row in df.itertuples():
                row_time = getattr(row, "time")
                if row_time == "time":
                    df.drop(index=[row[0], row[0]], inplace=True)
                    continue

                price = getattr(row, "price")
                volume = getattr(row, "amount")

                # check price and volume
                if price <= 0.0 or volume <= 0.0:
                    print(f"unexpected price: {price}, volume: {volume}, row: {row[0]}")
                    df.drop(index=[row[0], row[0]], inplace=True)

                # TODO: check price diff abnormal
    df.to_csv(output, index=False)
