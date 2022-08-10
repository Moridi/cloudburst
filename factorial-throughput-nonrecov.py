
from cloudburst.client.client import CloudburstConnection
import time
import threading
import sys
import redis

RUNNING = True

def writer(client, key, value):
    client.put(key, value)

def reader(client, key):
    return client.get(key)

def nonRecovFactorial(client, key, N, tid):
    for i in range(1, N + 1):
        value = client.get(key + str(tid))
        value *= i
        client.put(key + str(tid), value)    

def run_test(factorial, tid):
    global RUNNING
    while RUNNING:
        clear(tid)
        factorial("key", inputSize, tid).get()
        counters[tid] += 1
        
def clear(tid): 
    writerFuncs[tid](f"key_{tid}", 1).get()
    writerFuncs[tid](f"A_{tid}", 1).get()
    writerFuncs[tid](f"B_{tid}", 0).get()
    writerFuncs[tid](f"C_{tid}", 1).get()
    
    
def stop(): 
    global RUNNING
    RUNNING = False
    
  
def reportCheckpoints(): 
    print(",".join([str(NUM_THREADS), str(sum(counters)), str(sum(counters) / NUM_THREADS), str(counters)]))

inputSize = 200
ip = "localhost"
NUM_THREADS = int(sys.argv[1])
EXEC_TAG = "999998"


counters = [0] * NUM_THREADS
clients = [CloudburstConnection(ip, ip, local=True, tid=tid) for tid in range(NUM_THREADS)]
funcs = [clients[tid].register(nonRecovFactorial, f"throughputNonRecovFactorial_{EXEC_TAG}_{tid}") for tid in range(NUM_THREADS)]


writerFuncs = [client.register(writer, f"throughputWriter_{tid}") for tid, client in enumerate(clients)]
readerFuncs = [client.register(reader, f"throughputReader_{tid}") for tid, client in enumerate(clients)]


threads = [None] * NUM_THREADS

for tid, func in enumerate(funcs): 
    threads[tid] = threading.Thread(target=run_test, args=(func, tid))
    threads[tid].start()
    
    
timer = threading.Timer(60, stop)
timer.start()
    
for thread in threads: 
    thread.join()
    
reportCheckpoints()