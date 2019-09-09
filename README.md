# ProcessData

# clean zhubi data
python cleanzhubi.py $coin1_zhubi_raw_path $coin1_zhubi_output

# clean tick data
python cleantick.py $coin1_tick_raw_path $coin1_tick_output

# merge tick and zhubi
python mergetickzhubi.py $coin1_tick_output $coin1_zhubi_output $coin1_output

# merge different coins
python merge3coins $coin1_output $coin2_output $coin3_output $output

# csv to pickle
python csvtopickle.py $output $pickle_output
