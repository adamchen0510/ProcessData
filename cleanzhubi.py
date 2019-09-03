#!/usr/bin/env python
# coding: utf-8

import sys
import gzip
import os
import pandas as pd

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
        if os.path.isfile(path) and not path.endswith(".gz"):
            print(f"file is:{path}")
            df_temp = pd.read_csv(path, names=["exchange_time", "contract", "price", "bs", "amount", "exchange_timestamp", "time", "timestamp"])
            df = pd.concat([df, df_temp])
            for row in df.itertuples():
                # check price and volume
                price = getattr(row, "price")
                volume = getattr(row, "amount")
                if price <= 0 or volume <= 0.0:
                    print(f"unexpected price: {price}, volume: {volume}, row: {row[0]}")
                    df.drop(index=[row[0], row[0]], inplace=True)
    df.to_csv(output, index=False)
