
from cloudburst.client.client import CloudburstConnection
import time
import threading
import sys

def writer(client, key, value):
    client.put(key, value)

def reader(client, key):
    return client.get(key)

def nonRecovFactorial(client, key, N):
    for i in range(1, N + 1):
        value = client.get(key)
        value *= i
        client.put(key, value)    

def recovFactorial(client, key, N, tid):
    # Recovery phase
    if ((client.get(key + tid) == client.get("C" + tid))\
        and client.get("A" + tid) == client.get("B" + tid)):
        
        start = client.get("A" + tid) + 1
    else:
        start = client.get("A" + tid)
    
    # Execution
    for i in range(start, N + 1):
        client.put("A" + tid, i)
        
        value = client.get(key + tid)
        value *= i
        
        client.put("C" + tid, value)
        client.put("B" + tid, i)
        client.put(key + tid, value)

def run_test(factorial, tid):
    before = time.time()
    factorial("key", 5000, "_" + str(tid)).get()
    after = time.time()

    elapsedTime[tid] = after - before
    
def reportCheckpoints():
    sameOutput = True
    output = readerFunc("key_0").get()
    
    for tid in range(NUM_THREADS):
        lastOutput = readerFunc("key_" + str(tid)).get()
        
        sameOutput = sameOutput and (output == lastOutput)
        output = lastOutput
    
    print(sum(elapsedTime) / NUM_THREADS)
    
def clear():
    for i in range(NUM_THREADS):
        tid = str(i)
        writerFunc("key_" + tid, 1).get()
        writerFunc("A_" + tid, 1).get()
        writerFunc("B_" + tid, 0).get()
        writerFunc("C_" + tid, 1).get()
    
ip = "localhost"
NUM_THREADS = int(sys.argv[1])
EXEC_TAG = "007"

elapsedTime = [0] * NUM_THREADS
clients = [CloudburstConnection(ip, ip, local=True, tid=i) for i in range(NUM_THREADS)]
funcs = [clients[tid].register(recovFactorial, "recovFactorial_" + EXEC_TAG + "_" + str(tid))\
    for tid in range(NUM_THREADS)]

writerFunc = clients[0].register(writer, "writer")
readerFunc = clients[0].register(reader, "reader")

clear()
threads = [None] * NUM_THREADS

for tid, func in enumerate(funcs):    
    threads[tid] = threading.Thread(target=run_test, args=(func, tid))
    threads[tid].start()
    
for thread in threads:
    thread.join()
    
reportCheckpoints()