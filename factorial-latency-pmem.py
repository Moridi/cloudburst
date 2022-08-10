
from cloudburst.client.client import CloudburstConnection
import time
import threading
import sys

def clearFunc(client, tid):
    client.put("key_" + str(tid), 1)
    client.pmem_put_A(1, tid=tid)
    client.pmem_put_B(0, tid=tid)
    client.pmem_put_C(1, tid=tid)
    
def reportCheckpointsFunc(client, tid):
    result = ""
    result += "A_{}: {}\n".format(tid, client.pmem_get_A(tid))
    result += "B_{}: {}\n".format(tid, client.pmem_get_B(tid))
    result += 'key_{}: {}\n'.format(tid, str(client.get("key_" + str(tid)))[:5])
    result += 'C_{}: {}\n'.format(tid, str(client.pmem_get_C(tid))[:5])
    
    return result

def nonRecovFactorial(client, key, N):
    for i in range(1, N + 1):
        value = client.get(key)
        value *= i
        client.put(key, value)

def recovFactorial(client, key, N, tid):
    # Recovery phase    
    if ((client.get(key) == client.pmem_get_C(tid))\
        and client.pmem_get_A(tid) == client.pmem_get_B(tid)):

        start = client.pmem_get_A(tid) + 1
    else:
        start = client.pmem_get_A(tid)
    
    checkpointOverhead = 0.0
    value = 1
    # Execution
    for i in range(start, N + 1):
        
        startCheckpoint = time.time()
        client.pmem_put_A(i, tid=tid)
        endCheckpoint = time.time()
        checkpointOverhead += endCheckpoint - startCheckpoint
        
        value = client.get(key)
        value *= i
        
        startCheckpoint = time.time()
        client.pmem_put_C(value, tid=tid)
        client.pmem_put_B(i, tid=tid)
        endCheckpoint = time.time()
        checkpointOverhead += endCheckpoint - startCheckpoint
        
        client.put(key, value)
        
    return checkpointOverhead

def run_test(factorial, tid):
    start = time.time()
    checkpoints[tid] = factorial("key_" + str(tid), inputSize, tid).get()
    end = time.time()

    elapsedTimes[tid] = end - start
    
def reportCheckpoints():
    avgCheckpoints = sum(checkpoints) / NUM_THREADS
    avgElapsedTimes = sum(elapsedTimes) / NUM_THREADS
    
    print(NUM_THREADS, ",", avgCheckpoints, ",", avgElapsedTimes, ",",
          avgCheckpoints / avgElapsedTimes * 100)
    
ip = "localhost"
NUM_THREADS = int(sys.argv[1])
EXEC_TAG = "0100"
checkpoints = [0] * NUM_THREADS
elapsedTimes = [0] * NUM_THREADS
threads = [None] * NUM_THREADS
inputSize = 250

clients = [CloudburstConnection(ip, ip, local=True, tid=i) for i in range(NUM_THREADS)]
funcs = [clients[tid].register(recovFactorial, "recovFactorial_" + EXEC_TAG + "_" + str(tid))\
    for tid in range(NUM_THREADS)]

clears = [clients[tid].register(clearFunc, "clearFunc_"  + EXEC_TAG + "_" + str(tid)) for tid in range(NUM_THREADS)]
reportCheckpointsCBs = [clients[tid].register(reportCheckpointsFunc, "reportCheckpointsFunc_"  + EXEC_TAG + "_" + str(tid)) for tid in range(NUM_THREADS)]

for i, clear in enumerate(clears):
    clear(i).get()

for tid, func in enumerate(funcs):
    threads[tid] = threading.Thread(target=run_test, args=(func, tid))
    threads[tid].start()
    
# for tid, func in enumerate(funcs):    
#     run_test(func, tid)
    
for thread in threads:
    thread.join()

# for i, reportCheckpointsCB in enumerate(reportCheckpointsCBs):
#     print(reportCheckpointsCB(i).get())

reportCheckpoints()