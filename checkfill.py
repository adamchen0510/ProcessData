#!/usr/bin/env python
import datetime
import math
import sys

import pandas as pd


def checkfillornot(marketdata,orderplacetime,orderdirection,orderplaceprice,orderduration,orderqueuenumber,contractname,orderunit):
    orderendtime = orderplacetime + orderduration

    filled_amount = 0
    tick_buy_price = tick_buy_volume = tick_sell_price = tick_sell_volume = 0.0
    prev_rowtime = ''
    prev_rowtime_set = False
    for row in marketdata.itertuples():
        # filter contractname
        contract = getattr(row, "contract")
        if contract.find(contractname) < 0:
            continue

        # filter tick data, update buy/sell price and volume
        if pd.isna(getattr(row, "bs")):
            # tick data
            tick_buy_price = getattr(row, "bid_0_p")
            tick_buy_volume = getattr(row, "bid_0_v")
            tick_sell_price = getattr(row, "ask_0_p")
            tick_sell_volume = getattr(row, "ask_0_v")
            # print(f"tick_buy_price: {tick_buy_price} tick_buy_volume: {tick_buy_volume} tick_sell_price:{tick_sell_price} tick_sell_volume: {tick_sell_volume}")
            continue

        row_time = getattr(row, "time")
        # filter repeat zhubi data
        if prev_rowtime_set and prev_rowtime >= row_time:
            continue
        prev_rowtime = row_time
        prev_rowtime_set = True
        row_datetime = pd.to_datetime(row_time)

        if orderplacetime < row_datetime < orderendtime:
            row_price = getattr(row, "price")
            # row_direction = getattr(row, "bs")
            row_amount = getattr(row, "amount")
            remain_amount = row_amount
            # print(f"Remain amount is {remain_amount}")
            if orderdirection == 'BUY':
                # consider tick queue first
                if math.isclose(tick_buy_price, orderplaceprice, abs_tol=0.000000001):
                    remain_amount -= tick_buy_volume
                    # print(f"2 Remain amount is {remain_amount}")

                if remain_amount <= 0:
                    continue

                if row_price <= orderplaceprice:
                    filled_amount += remain_amount

            elif orderdirection == "SELL":
                if math.isclose(tick_sell_price, orderplaceprice, abs_tol=0.000000001):
                    remain_amount -= tick_sell_volume
                    # print(f"3 Remain amount is {remain_amount}")

                if remain_amount <= 0:
                    continue

                if row_price >= orderplaceprice:
                    filled_amount += row_amount

            if filled_amount >= orderqueuenumber:
                print(f"Filled amount is {filled_amount} bigger than placed amount {orderqueuenumber}")
                return True

    print(f"Filled amount is {filled_amount} little than placed amount {orderqueuenumber}")
    return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage python checkfill.py $input.csv")
        exit(0)
    marketdata = pd.read_csv(sys.argv[1])
    orderplacetime = datetime.datetime(2019, 8, 1, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=0)))
    orderdirection = 'BUY'
    orderduration = datetime.timedelta(seconds=1200)
    orderplaceprice = 0.00020
    orderqueuenumber = 100000
    contractname = 'lrc.eth'
    filled = checkfillornot(marketdata, orderplacetime, orderdirection, orderplaceprice, orderduration, orderqueuenumber, contractname, 0)
    print(f"Filled value is {filled}")
