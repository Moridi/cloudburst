#!/bin/bash
NUM_OF_EXPR=6

echo "threads, checkpoint(s), execution(s), latency(%)" > latency-redis.csv
# for i in {1,4}
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
        python3 factorial-latency-redis.py $i >> latency-redis.csv
    done
done