
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

def readCheckpoint(r, obj, field): 
    return r.hget(obj, field)

def writeCheckpoint(r, obj, field, value): 
    r.hset(obj, field, value)

def nonRecovFactorial(client, key, N):
    for i in range(1, N + 1):
        value = client.get(key)
        value *= i
        client.put(key, value)    

def recovFactorial(client, key, N, tid):
    
    REDIS_OBJECT = f"MY_REDIS_OBJECT_{tid}"
    r = redis.Redis()
    r.set_response_callback('HGET', int)    
    
    # recovery phase, read all the checkpoints
    mykey = f"{key}_{tid}"
    if ((client.get(mykey) == readCheckpoint(r, REDIS_OBJECT, "C")) and readCheckpoint(r, REDIS_OBJECT, "A") == readCheckpoint(r, REDIS_OBJECT, "B")): 
        start = readCheckpoint(r, REDIS_OBJECT, "A") + 1
    else: 
        start = readCheckpoint(r, REDIS_OBJECT, "A")
    
    
    for i in range(start, N + 1): 
        writeCheckpoint(r, REDIS_OBJECT, "A", i)
        
        value = client.get(mykey)
        value *= i
        
        writeCheckpoint(r, REDIS_OBJECT, "C", value)
        writeCheckpoint(r, REDIS_OBJECT, "B", i)
        
        client.put(mykey, value)
        
        
def run_test(factorial, tid):
    global RUNNING
    while RUNNING:
        clear(tid)
        factorial("key", inputSize, tid).get()
        counters[tid] += 1


def clear(tid): 
    writerFuncs[tid](f"key_{tid}", 1).get()
    REDIS_OBJECT = f"MY_REDIS_OBJECT_{tid}"
    r = redis.Redis()
    # writerFuncs[tid](f"A_{tid}", 1).get()
    # writerFuncs[tid](f"B_{tid}", 0).get()
    # writerFuncs[tid](f"C_{tid}", 1).get()
    writeCheckpoint(r, REDIS_OBJECT, "A", 1)
    writeCheckpoint(r, REDIS_OBJECT, "B", 0)
    writeCheckpoint(r, REDIS_OBJECT, "C", 1)
    
def stop(): 
    global RUNNING
    RUNNING = False
    
def reportCheckpoints(): 
    print(",".join([str(NUM_THREADS), str(sum(counters)), str(sum(counters) / NUM_THREADS), str(counters)]))

inputSize = 200
ip = "localhost"
NUM_THREADS = int(sys.argv[1])
EXEC_TAG = "999999"

counters = [0] * NUM_THREADS
clients = [CloudburstConnection(ip, ip, local=True, tid=tid) for tid in range(NUM_THREADS)]
funcs = [clients[tid].register(recovFactorial, f"throughputRecovFactorial_{EXEC_TAG}_{tid}") for tid in range(NUM_THREADS)]

writerFuncs = [client.register(writer, f"throughputWriter_{tid}") for tid, client in enumerate(clients)]
readerFuncs = [client.register(reader, f"throughputReader_{tid}") for tid, client in enumerate(clients)]

writerFunc = clients[0].register(writer, "throughputWriter")
readerFunc = clients[0].register(reader, "throughputReader")

threads = [None] * NUM_THREADS

for tid, func in enumerate(funcs): 
    threads[tid] = threading.Thread(target=run_test, args=(func, tid))
    threads[tid].start()
    
    
timer = threading.Timer(60, stop)
timer.start()
    
for thread in threads: 
    thread.join()
    
reportCheckpoints()