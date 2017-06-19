export LD_LIBRARY_PATH=/usr/local/mysql/lib/mysql/
DBNAME="tpcc1"
WH=500
HOST=172.16.73.173
PORT=5555
STEP=100

./tpcc_load -h $HOST -P $PORT -d $DBNAME -u dbscale -p "dbscale" -w $WH -l 1 -m 1 -n $WH >> 1.out &

x=1

while [ $x -le $WH ]
do
 echo $x $(( $x + $STEP - 1 ))
./tpcc_load -h $HOST -P $PORT -d $DBNAME -u dbscale -p "dbscale" -w $WH -l 2 -m $x -n $(( $x + $STEP - 1 ))  >> 2_$x.out &
./tpcc_load -h $HOST -P $PORT -d $DBNAME -u dbscale -p "dbscale" -w $WH -l 3 -m $x -n $(( $x + $STEP - 1 ))  >> 3_$x.out &
./tpcc_load -h $HOST -P $PORT -d $DBNAME -u dbscale -p "dbscale" -w $WH -l 4 -m $x -n $(( $x + $STEP - 1 ))  >> 4_$x.out &
 x=$(( $x + $STEP ))
done

