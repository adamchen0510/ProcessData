time python cleanzhubi.py raw_data/okex/lrc_eth/zhubi/ lrc_zhubi.csv
time python cleantick.py raw_data/okex/lrc_eth/tick/ lrc_tick.csv
time python cleanzhubi.py raw_data/okex/eth_usdt/zhubi/ eth_zhubi.csv
time python cleantick.py raw_data/okex/eth_usdt/tick/ eth_tick.csv

time python mergetickzhubi.py lrc_tick.csv lrc_zhubi.csv lrc_output_raw.csv > logo3
time python mergetickzhubi.py eth_tick.csv eth_zhubi.csv eth_output_raw.csv > logo4

time python correcttime.py lrc_output_raw.csv lrc_output.csv > logo31
time python correcttime.py eth_output_raw.csv eth_output.csv > logo32

time python merge2coins.py eth_output.csv lrc_output.csv okex.csv > logo6

time python csvtopickle.py okex.csv okex.pickle > logo7
