#!/bin/bash
NUM_OF_EXPR=1

echo "threads, checkpoint(s), execution(s), latency(%)" > latency-pmem-ssd.csv
# for i in {1,4}
for i in {1,4,8,12,16,20,24}
do
    for (( j=1; j<=$NUM_OF_EXPR; j++ ))
    do
        THREADS=$i
        ./kill_cloudburst.sh
        sleep 2
        ./scripts/start-cloudburst-local-multithread.sh y y $i
        sleep 2
        avg=0
        python3 factorial-latency-pmem.py $i >> latency-pmem-ssd.csv
    done
done

./kill_cloudburst.sh