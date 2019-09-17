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
        print(f"Usage python checkscv.py $inputFile $outputFile")
        exit(0)
    input_csv = sys.argv[1]
    output_csv = sys.argv[2]
    print(f"input csv: {input_csv} output_csv: {output_csv}")

    df = pd.read_csv(input_csv)

    prev_time = ''
    prev_set = False
    for row in df.itertuples():
        row_time = getattr(row, "time")
        if row_time == "time":
            continue
        abnormal = getattr(row, "IsDataNormal")
        if abnormal == 0:
            print(f"Abnormal: row index:{row[0]}")
        if prev_set:
            if row_time < prev_time:
                print(f"Invalid timestampe, pre_time: {prev_time} row index: {row[0]}, row: {row}")
                df.loc[row[0], 'time'] = pd.to_datetime(prev_time) + datetime.timedelta(microseconds=1)
                df.loc[row[0], 'IsDataNormal'] = 0
                continue
        prev_time = row_time
        prev_set = True

    df.to_csv(output_csv, index=False)
