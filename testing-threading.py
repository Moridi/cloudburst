import threading
import time

RUNNING = True
threads = [None] * 4
sums = [0] * 4

def func(tid):
    while RUNNING: 
        sums[tid] += 1

def stop(): 
    global RUNNING
    RUNNING = False

for tid, _ in enumerate(threads): 
    threads[tid] = threading.Thread(target=func, args=(tid,))
    threads[tid].start()

timer = threading.Timer(1, stop)
timer.start()

for thread in threads: 
    thread.join()

print(sums)