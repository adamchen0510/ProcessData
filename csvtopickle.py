#!/usr/bin/env python
# coding: utf-8
import datetime
import math
import pickle
import sys
import pandas as pd

import logfile

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage python csvtopickle.py $csv $outputFile")
        exit(0)
    csv_path = sys.argv[1]
    output = sys.argv[2]

    input_csv = pd.read_csv(csv_path)
    input_csv.info()

    input_csv.to_pickle(output)
    # pickle.dump(input_csv, open(output))
