
for((i=2;i<=7;i++));
do
sed -i "s/"-01"/"-0$i"/g" util.py
./run_binance.sh
rm  *_raw.csv
mkdir $i
mv *.csv $i
tar cvzf $i.tgz $i
git checkout util.py
done

