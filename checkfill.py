#!/usr/bin/env python
import datetime
import sys

import pandas as pd


def checkfillornot(marketdata,orderplacetime,orderdirection,orderplaceprice,orderduration,orderqueuenumber,contractname,orderunit):
    orderendtime = orderplacetime + orderduration

    filled_amount = 0
    for row in marketdata.itertuples():
        row_time = getattr(row, "time")
        row_datetime = pd.to_datetime(row_time)
        if orderplacetime < row_datetime < orderendtime:
            row_price = getattr(row, "price")
            row_direction = getattr(row, "bs")
            row_amount = getattr(row, "amount")
            if orderdirection == 'BUY' and row_direction == "s":
                if row_price <= orderplaceprice:
                    filled_amount += row_amount

            elif orderdirection == "SELL" and row_direction == "b":
                if row_price >= orderplaceprice:
                    filled_amount += row_amount

            if filled_amount >= orderqueuenumber:
                print(f"Amount is {filled_amount}")
                return True

    return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage python checkfill.py $input.csv")
        exit(0)
    marketdata = pd.read_csv(sys.argv[1])
    orderplacetime = datetime.datetime(2019, 8, 1, 10, tzinfo=datetime.timezone(datetime.timedelta(hours=0)))
    orderdirection = 'BUY'
    orderduration = datetime.timedelta(seconds=120)
    orderplaceprice = 0.000216
    orderqueuenumber = 1000
    contractname = 'lrc.eth'
    filled = checkfillornot(marketdata, orderplacetime, orderdirection, orderplaceprice, orderduration, orderqueuenumber, contractname, 0)
    print(f"Filled value is {filled}")
