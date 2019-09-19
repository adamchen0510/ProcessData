time python cleanzhubi.py raw_data/binance/lrc_eth/zhubi/ lrc_zhubi.csv
time python cleantick.py raw_data/binance/lrc_eth/tick/ lrc_tick.csv
time python cleanzhubi.py raw_data/binance/eth_usdt/zhubi/ eth_zhubi.csv
time python cleantick.py raw_data/binance/eth_usdt/tick/ eth_tick.csv
time python cleanzhubi.py raw_data/binance/btc_usdt/zhubi/ btc_zhubi.csv
time python cleantick.py raw_data/binance/btc_usdt/tick/ btc_tick.csv

time python mergetickzhubi.py lrc_tick.csv lrc_zhubi.csv lrc_output_raw.csv > logb3
time python mergetickzhubi.py eth_tick.csv eth_zhubi.csv eth_output_raw.csv > logb4
time python mergetickzhubi.py btc_tick.csv btc_zhubi.csv btc_output_raw.csv > logb5

time python correcttime.py lrc_output_raw.csv lrc_output.csv > logb31
time python correcttime.py eth_output_raw.csv eth_output.csv > logb32
time python correcttime.py btc_output_raw.csv btc_output.csv > logb33

time python merge3coins.py btc_output.csv eth_output.csv lrc_output.csv binance.csv > logb6

time python csvtopickle.py binance.csv binance.pickle > logb7
