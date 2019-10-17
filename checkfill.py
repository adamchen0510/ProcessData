#!/usr/bin/env python
import datetime
import math
import sys
import time

import pandas as pd

# little and equal
def isLE(first, second):
    if first < second:
        return True
    if math.isclose(first, second, abs_tol=0.000000001):
        return True
    return False

# bigger and equal
def isBE(first, second):
    if first > second:
        return True
    if math.isclose(first, second, abs_tol=0.000000001):
        return True
    return False

def calc_fill_tick_amount(this_tick, next_tick, p, orderdirection):
    this_ask = this_tick[1]
    this_ask_v = this_tick[2]
    next_ask = next_tick[1]
    next_ask_v = next_tick[2]
    filled_amount = 0
    if orderdirection == "BUY":
        if this_ask > next_ask:
            if isBE(p, this_ask):
                filled_amount = next_ask_v + this_ask_v
            elif p < next_ask:
                # do nothing
                filled_amount = filled_amount
            elif isBE(p, next_ask) and p < this_ask:
                filled_amount = next_ask_v
        elif this_ask == next_ask:
            filled_amount = next_ask_v
        elif this_ask < next_ask:
            if isBE(p, next_ask):
                filled_amount = next_ask_v
            elif p < this_ask:
                # do nothing
                filled_amount = filled_amount
            elif isBE(p, this_ask) and p < next_ask:
                # do nothing
                filled_amount = filled_amount
    elif orderdirection == "SELL":
        filled_amount = filled_amount

    return filled_amount

def calc_fill_zhubi_amount(this_tick, next_tick, p, orderdirection, zhubi_p, zhubi_v):
    this_ask = this_tick[1]
    this_ask_v = this_tick[2]
    next_ask = next_tick[1]
    next_ask_v = next_tick[2]
    filled_amount = 0
    if orderdirection == "BUY":
        if this_ask > next_ask:
            if isBE(p, this_ask):
                if zhubi_p < p:
                    filled_amount = zhubi_v
            elif p < next_ask:
                if zhubi_p < p:
                    filled_amount = zhubi_v
            elif isBE(p, next_ask) and p < this_ask:
                if zhubi_p < p:
                    filled_amount = zhubi_v
        elif this_ask == next_ask:
            if zhubi_p < p:
                filled_amount = zhubi_v
        elif this_ask < next_ask:
            if isBE(p, next_ask):
                if zhubi_p < p:
                    filled_amount = zhubi_v
            elif p < this_ask:
                if zhubi_p < p:
                    filled_amount = zhubi_v
            elif isBE(p, this_ask) and p < next_ask:
                if zhubi_p < p:
                    filled_amount = zhubi_v
    elif orderdirection == "SELL":
        filled_amount = filled_amount

    return filled_amount


def checkfillornot(marketdata,orderplacetime,orderdirection,orderplaceprice,orderduration,orderqueuenumber,contractname,orderunit):
    orderendtime = orderplacetime + orderduration

    filled_amount = 0
    tick_data = []
    prev_rowtime = ''
    prev_rowtime_set = False
    filtered_marketdata = marketdata
    # filtered_marketdata = filtered_marketdata.iloc[6000:]
    # filter contractName
    filtered_marketdata = filtered_marketdata[filtered_marketdata['contract'] == contractname]

    # filter from start time
    filtered_marketdata = filtered_marketdata[filtered_marketdata['time'] >= orderplacetime.__str__()]

    # filter tick data
    filtered_tickdata = filtered_marketdata[pd.isna(filtered_marketdata["bs"])]

    # find next tick time after orderendtime and get all tick data
    for row in filtered_tickdata.itertuples():
        # print(f"tick row is {row}")
        row_time = getattr(row, "time")
        # filter repeated tick row
        if prev_rowtime_set and prev_rowtime >= row_time:
            continue
        prev_rowtime = row_time
        prev_rowtime_set = True
        ask_p = getattr(row, "ask_0_p")
        ask_v = getattr(row, "ask_0_v")
        bid_p = getattr(row, "bid_0_p")
        bid_v = getattr(row, "bid_0_v")
        tick_data.append([row_time, ask_p, ask_v, bid_p, bid_v])
        if row_time >= orderendtime.__str__():
            # reset orderendtime
            orderendtime = pd.to_datetime(row_time)
            break

    print(f"tick_data is {tick_data}")

    # filter to end time
    filtered_marketdata = filtered_marketdata[filtered_marketdata['time'] <= orderendtime.__str__()]

    # filter without tick data
    filtered_marketdata = filtered_marketdata[pd.isna(filtered_marketdata["last"])]

    tick_data_index = 0
    this_tick = tick_data[tick_data_index]
    next_tick = tick_data[tick_data_index + 1]
    filled_amount += calc_fill_tick_amount(this_tick, next_tick, orderplaceprice, orderdirection)
    prev_rowtime = ''
    prev_rowtime_set = False
    if isBE(filled_amount, orderqueuenumber):
        print(f"Filled amount is {filled_amount} bigger and equal than placed amount {orderqueuenumber}")
        return True
    for row in filtered_marketdata.itertuples():
        row_time = getattr(row, "time")
        if row_time < orderplacetime.__str__():
            continue
        elif row_time > orderendtime.__str__():
            break
        # filter repeat zhubi data
        if prev_rowtime_set and prev_rowtime >= row_time:
            continue
        prev_rowtime = row_time
        prev_rowtime_set = True

        if row_time > next_tick[0]:
            if tick_data_index + 1 + 1 > len(tick_data):
                print("Reach tick data end, break")
                break
            tick_data_index += 1
            this_tick = next_tick
            next_tick = tick_data[tick_data_index+1]
            filled_amount += calc_fill_tick_amount(this_tick, next_tick, orderplaceprice, orderdirection)
            if isBE(filled_amount, orderqueuenumber):
                print(f"Filled amount is {filled_amount} bigger and equal than placed amount {orderqueuenumber}")
                return True

        if this_tick[0] <= row_time <= next_tick[0]:
            row_price = getattr(row, "price")
            row_amount = getattr(row, "amount")
            filled_amount += calc_fill_zhubi_amount(this_tick, next_tick, orderplaceprice, orderdirection, row_price, row_amount)

            if isBE(filled_amount, orderqueuenumber):
                print(f"Filled amount is {filled_amount} bigger and equal than placed amount {orderqueuenumber}")
                return True

    if isBE(filled_amount, orderqueuenumber):
        print(f"Filled amount is {filled_amount} bigger and equal than placed amount {orderqueuenumber}")
        return True
    print(f"Filled amount is {filled_amount} little than placed amount {orderqueuenumber}")
    return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage python checkfill.py $input.csv")
        exit(0)
    marketdata = pd.read_csv(sys.argv[1])
    orderplacetime = datetime.datetime(2019, 8, 1, 0, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=0)))
    orderdirection = 'BUY'
    orderduration = datetime.timedelta(seconds=1200)
    orderplaceprice = 0.00019642
    orderqueuenumber = 100000
    contractname = 'lrc.eth:xtc.binance'
    print("current time: " + str(time.time()))
    filled = checkfillornot(marketdata, orderplacetime, orderdirection, orderplaceprice, orderduration, orderqueuenumber, contractname, 0)
    print("current time: " + str(time.time()))
    print(f"Filled value is {filled}")
