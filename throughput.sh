#!/bin/bash
NUM_OF_EXPR=4

echo "threads,sum,avg,per_thread" > throughput.csv

for i in {1,4,8,12,16,20,24}
do
    THREADS=$i
    ./kill_cloudburst.sh
    sleep 2
    ./scripts/start-cloudburst-local-multithread.sh y y $i
    sleep 2
    avg=0

    for (( j=1; j<=$NUM_OF_EXPR; j++ ))
    do
        python3 factorial-throughput.py $i >> throughput.csv
    done
done