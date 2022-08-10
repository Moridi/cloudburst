from cloudburst.client.client import CloudburstConnection
import time
import threading
import sys
import redis


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
    checkpointOverhead = 0.0
    
    startCheckpoint = time.time() 
    REDIS_OBJECT = f"MY_REDIS_OBJECT_{tid}"
    r = redis.Redis()
    r.set_response_callback('HGET', int)    
    endCheckpoint = time.time()
    checkpointOverhead += endCheckpoint - startCheckpoint
    
    
    # recovery phase, read all the checkpoints
    startCheckpoint = time.time()
    mykey = f"{key}_{tid}"
    if ((client.get(mykey) == readCheckpoint(r, REDIS_OBJECT, "C")) and readCheckpoint(r, REDIS_OBJECT, "A") == readCheckpoint(r, REDIS_OBJECT, "B")): 
        start = readCheckpoint(r, REDIS_OBJECT, "A") + 1
    else: 
        start = readCheckpoint(r, REDIS_OBJECT, "A")
    
    endCheckpoint = time.time()
    checkpointOverhead += endCheckpoint - startCheckpoint
    
    for i in range(start, N + 1): 
        startCheckpoint = time.time()
        writeCheckpoint(r, REDIS_OBJECT, "A", i)
        endCheckpoint = time.time()
        checkpointOverhead += endCheckpoint - startCheckpoint
        
        value = client.get(mykey)
        value *= i
        
        startCheckpoint = time.time()
        writeCheckpoint(r, REDIS_OBJECT, "C", value)
        writeCheckpoint(r, REDIS_OBJECT, "B", i)
        endCheckpoint = time.time()
        checkpointOverhead += endCheckpoint - startCheckpoint
        
        client.put(mykey, value)
        
    return checkpointOverhead


def run_test(factorial, tid): 
    start = time.time()
    checkpoints[tid] = factorial("key", inputSize, tid).get()
    end = time.time()
    elapsedTimes[tid] = end - start


def reportCheckpoints():
    avgCheckpoints = sum(checkpoints) / NUM_THREADS
    avgElapsedTimes = sum(elapsedTimes) / NUM_THREADS
    
    print(NUM_THREADS, ",", avgCheckpoints, ",", avgElapsedTimes, ",",
          avgCheckpoints / avgElapsedTimes * 100)

def clear():
    r = redis.Redis()
    r.set_response_callback('HGET', int)   
        
    for tid in range(NUM_THREADS): 
        writerFunc(f"key_{tid}", 1).get()
        REDIS_OBJECT = f"MY_REDIS_OBJECT_{tid}"
        writeCheckpoint(r, REDIS_OBJECT, "A", 1)
        writeCheckpoint(r, REDIS_OBJECT, "B", 0)
        writeCheckpoint(r, REDIS_OBJECT, "C", 1)
        

ip = "localhost"
NUM_THREADS = int(sys.argv[1])
EXEC_TAG = "997"
checkpoints = [0] * NUM_THREADS
elapsedTimes = [0] * NUM_THREADS
inputSize = 250

clients = [CloudburstConnection(ip, ip, local=True, tid=tid) for tid in range(NUM_THREADS)]
funcs = [clients[tid].register(recovFactorial, f"redisRecovFactorial_{EXEC_TAG}_{tid}") for tid in range(NUM_THREADS)]

writerFunc = clients[0].register(writer, "redisWriter")
readerFunc = clients[0].register(reader, "redisReader")

clear()
threads = [None] * NUM_THREADS
for tid, func in enumerate(funcs): 
    threads[tid] = threading.Thread(target=run_test, args=(func, tid))
    threads[tid].start()
    
for tid, thread in enumerate(threads): 
    thread.join()
    print(str(readerFunc(f"key_{tid}").get())[:5])    
reportCheckpoints()
