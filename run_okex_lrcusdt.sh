time python cleanzhubi.py raw_data/okex/lrc_usdt/zhubi/ lrc_usdt_zhubi.csv
time python cleantick.py raw_data/okex/lrc_usdt/tick/ lrc_usdt_tick.csv

time python mergetickzhubi.py lrc_usdt_tick.csv lrc_usdt_zhubi.csv lrc_usdt_output_raw.csv > logo3

time python correcttime.py lrc_usdt_output_raw.csv lrc_usdt_output.csv > logo31

time python csvtopickle.py lrc_usdt_output.csv lrc_usdt_output.pickle > logo7
