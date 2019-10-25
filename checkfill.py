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

def calc_filled_amount(this_tick, next_tick, p, orderdirection, zhubi_p, zhubi_v, is_tick):
    this_ask = this_tick[1]
    this_ask_v = this_tick[2]
    next_ask = next_tick[1]
    next_ask_v = next_tick[2]
    this_bid = this_tick[3]
    this_bid_v = this_tick[4]
    next_bid = next_tick[3]
    next_bid_v = next_tick[4]
    filled_amount = 0
    if orderdirection == "BUY":
        if not is_tick:
            if isLE(zhubi_p, p):
                filled_amount = zhubi_v
            return filled_amount
        if this_ask > next_ask:
            if isBE(p, this_ask):
                filled_amount = next_ask_v + this_ask_v
            elif p < next_ask:
                # do nothing
                filled_amount = filled_amount
            elif isBE(p, next_ask) and p < this_ask:
                filled_amount = next_ask_v
        elif this_ask == next_ask:
            if isBE(p, this_ask):
                filled_amount = next_ask_v
            elif p > this_ask:
                # do nothing
                filled_amount = filled_amount
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
        if not is_tick:
            if isBE(zhubi_p, p):
                filled_amount = zhubi_v
            return filled_amount
        if this_bid < next_bid:
            if isLE(p, next_bid):
                filled_amount = next_bid_v + this_bid_v
            elif p > next_bid:
                # do nothing
                filled_amount = filled_amount
            elif isLE(p, next_bid) and p > this_bid:
                filled_amount = next_bid_v
        elif this_bid == next_bid:
            if isLE(p, next_bid):
                filled_amount = next_bid_v
            elif p > next_bid:
                # do nothing
                filled_amount = filled_amount
        elif this_bid > next_bid:
            if isLE(p, next_bid):
                filled_amount = next_bid_v
            elif p > this_bid:
                # do nothing
                filled_amount = filled_amount
            elif isLE(p, this_bid) and p > next_bid:
                # do nothing
                filled_amount = filled_amount
    return filled_amount

def checkfillornot_single(filtered_tickdata,filtered_marketdata,orderplacetime,orderdirection,orderplaceprice,
                          orderendtime,orderqueuenumber,contractname):
    filled_amount = 0
    tick_data = []
    prev_rowtime = ''
    prev_rowtime_set = False
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
        if row_time >= orderendtime.__str__() and len(tick_data) >= 2:
            # do not reset orderendtime
            # orderendtime = pd.to_datetime(row_time)
            break

    # print(f"tick_data is {tick_data}")
    if len(tick_data) < 2:
        print(f"Invalid tick data len: {len(tick_data)}, return False")
        # print(f"placd time {orderplacetime} duration {orderduration}")
        return False

    # filter to end time
    filtered_marketdata = filtered_marketdata[filtered_marketdata['time'] <= orderendtime.__str__()]

    # filter without tick data
    filtered_marketdata = filtered_marketdata[pd.isna(filtered_marketdata["last"])]

    tick_data_len = 0
    this_tick = tick_data[tick_data_len - 2]
    next_tick = tick_data[tick_data_len - 1]
    filled_amount += calc_filled_amount(this_tick, next_tick, orderplaceprice, orderdirection, 0.0, 0.0, True)
    if isBE(filled_amount, orderqueuenumber):
        print(f"Filled amount is {filled_amount} bigger and equal than placed amount {orderqueuenumber}")
        return True
    last_2_tick_time = tick_data[len(tick_data) - 2][0]
    prev_rowtime = ''
    prev_rowtime_set = False
    for row in filtered_marketdata.itertuples():
        # print(f"row is: {row}")
        row_time = getattr(row, "time")
        if row_time < orderplacetime.__str__():
            continue
        elif row_time > orderendtime.__str__():
            break
        # filter repeat data
        if prev_rowtime_set and prev_rowtime >= row_time:
            continue

        prev_rowtime = row_time
        prev_rowtime_set = True
        tick_buy_price = tick_buy_volume = tick_sell_price = tick_sell_volume = 0.0

        row_price = getattr(row, "price")
        row_amount = getattr(row, "amount")
        if row_time >= last_2_tick_time:
            # skip tick data
            # if pd.isna(getattr(row, "bs")):
            #     continue
            filled_amount += calc_filled_amount(this_tick, next_tick, orderplaceprice, orderdirection, row_price,
                                                row_amount, False)
        else:
            is_tick = False
            # if pd.isna(getattr(row, "bs")):
            #     tick_buy_price = getattr(row, "bid_0_p")
            #     tick_buy_volume = getattr(row, "bid_0_v")
            #     tick_sell_price = getattr(row, "ask_0_p")
            #     tick_sell_volume = getattr(row, "ask_0_v")
            #     is_tick = True
            if orderdirection == 'BUY':
                if is_tick and isBE(orderplaceprice, tick_sell_price):
                    filled_amount += tick_sell_volume

                if not is_tick and isBE(orderplaceprice, row_price):
                    filled_amount += row_amount

            elif orderdirection == "SELL":
                if is_tick and isLE(orderplaceprice, tick_buy_price):
                    filled_amount += tick_buy_volume

                if not is_tick and isLE(orderplaceprice, row_price):
                    filled_amount += row_amount

        if isBE(filled_amount, orderqueuenumber):
            print(f"Filled amount is {filled_amount} bigger and equal than placed amount {orderqueuenumber}")
            return True

    print(f"Filled amount is {filled_amount} little than placed amount {orderqueuenumber}")
    return False

def checkfillornot(marketdata,orderplacetime,orderdirection,orderplaceprices,orderdurations,orderqueuenumbers,contractname,orderunit):
    filled_status = []
    filtered_marketdata = marketdata
    # duration_row_num = marketdata.shape[0]#5000
    # abc = marketdata.loc[marketdata['time'] == orderplacetime.__str__()]
    # start_row_num = abc.index.values[0]
    # end_row_num = start_row_num + duration_row_num
    # if end_row_num >= marketdata.shape[0]:
    #     end_row_num = marketdata.shape[0]
    # filtered_marketdata = marketdata[start_row_num:]
    # filter contractName
    # filtered_marketdata = filtered_marketdata[filtered_marketdata['contract'] == contractname]

    # filter from start time
    # filtered_marketdata = filtered_marketdata[filtered_marketdata['time'] >= orderplacetime.__str__()]

    # filter tick data
    filtered_tickdata = filtered_marketdata[pd.isna(filtered_marketdata["bs"])]
    for i in range(0, len(orderplaceprices)):
        filled_status.append(checkfillornot_single(filtered_tickdata,filtered_marketdata,orderplacetime,orderdirection,orderplaceprices[i],
                              orderplacetime+orderdurations[i],orderqueuenumbers[i],contractname))

    return filled_status


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage python checkfill.py $input.csv")
        exit(0)
    marketdata = pd.read_csv(sys.argv[1])
    # marketdata = pd.read_csv("binance.csv")
    orderplacetime = datetime.datetime(2019, 8, 1, 0, 9, 2, tzinfo=datetime.timezone(datetime.timedelta(hours=0)))
    orderdirection = 'BUY'
    orderduration = [datetime.timedelta(seconds=600),datetime.timedelta(seconds=1200)]
    # orderplaceprice = [0.00019642, 0.00019642]
    # orderqueuenumber = [5000, 10000]
    # contractname = 'lrc.eth:xtc.binance'
    orderplaceprice = [10000, 8000]
    orderqueuenumber = [0.1, 0.2]
    contractname = 'btc.usdt:xtc.binance'
    filtered_marketdata = marketdata[marketdata['contract'] == contractname].reset_index()
    tick_data = filtered_marketdata[pd.isna(filtered_marketdata["bs"])]
    index = 0
    row_delta = 100000
    time_cost = 0.0
    prev_rowtime = ''
    prev_rowtime_set = False
    print("current time: " + str(time.time()))
    for row in tick_data.itertuples():
        row_time = pd.to_datetime(getattr(row, "time"))
        # print(f"row is {row}")
        # filter repeat tick data
        if prev_rowtime_set and prev_rowtime >= row_time:
            continue
        prev_rowtime = row_time
        prev_rowtime_set = True
        orderplacetime = row_time
        start_row = filtered_marketdata.loc[filtered_marketdata['time'] == orderplacetime.__str__()]
        # print(f"start row is : {start_row}")
        start_row_num = start_row.index.values[0]
        end_row_num = start_row_num + row_delta
        if end_row_num > filtered_marketdata.shape[0]:
            end_row_num = filtered_marketdata.shape[0]
        temp_marketdata = filtered_marketdata[start_row_num:]#filtered_marketdata[start_row_num:end_row_num]
        start_fill_time = time.time()
        filled = checkfillornot(temp_marketdata, orderplacetime, orderdirection, orderplaceprice, orderduration, orderqueuenumber, contractname, 0)
        time_cost += time.time() - start_fill_time
        print(f"Filled value is {filled}")
        print(f"progress: {start_row_num/filtered_marketdata.shape[0]}")
        index += 1
    print(f"tick num is {index}")
    print(f"time cost is {time_cost}")
    print("current time: " + str(time.time()))
