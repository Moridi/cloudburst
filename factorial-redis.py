
from asyncore import read
from cloudburst.client.client import CloudburstConnection
import time
import redis

ip = "localhost"
client = CloudburstConnection(ip, ip, local=True)

REDIS_OBJECT = "MY_REDIS_OBJECT"

def readCheckpoint(r, field): 
    return r.hget(REDIS_OBJECT, field)

def writeCheckpoint(r, field, value): 
    r.hset(REDIS_OBJECT, field, value)

def writer(client, key, value):
    client.put(key, value)

def reader(client, key):
    return client.get(key)

def nonRecovFactorial(client, key, N):
    for i in range(1, N + 1):
        value = client.get(key)
        value *= i
        client.put(key, value)    

def recovFactorial(client, key, N):
    r = redis.Redis()
    r.set_response_callback('HGET', int)
    
    # Recovery phase
    if ((client.get(key) == readCheckpoint(r, "C")) and readCheckpoint(r, "A") == readCheckpoint(r, "B")):
        start = readCheckpoint(r, "A") + 1
    else: # 2, 3, 4
        start = readCheckpoint(r, "A")
    
    writeCheckpoint(r, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        writeCheckpoint(r, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        writeCheckpoint(r, "C", value)
        # 3
        writeCheckpoint(r, "B", i)
        # 4
        client.put(key, value)

    writeCheckpoint(r, "isCompleted", 1)

def brokenFactorial1(client, key, N):
    r = redis.Redis()
    r.set_response_callback('HGET', int)
    
    # Recovery phase
    if (client.get(key) == readCheckpoint(r, "C")):
        if (readCheckpoint(r, "A") == readCheckpoint(r, "B")): # 1
            start = readCheckpoint(r, "A") + 1
        else: # 2
            start = readCheckpoint(r, "A")
    else: # 3, 4
        start = readCheckpoint(r, "A")
    
    writeCheckpoint(r, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        if (i == N // 3):
            return
        
        writeCheckpoint(r, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        writeCheckpoint(r, "C", value)
        # 3
        writeCheckpoint(r, "B", i)
        # 4
        client.put(key, value)

    writeCheckpoint(r, "isCompleted", 1)
    
def brokenFactorial2(client, key, N):
    r = redis.Redis()
    r.set_response_callback('HGET', int)
    
    # Recovery phase
    if (client.get(key) == readCheckpoint(r, "C")):
        if (readCheckpoint(r, "A") == readCheckpoint(r, "B")): # 1
            start = readCheckpoint(r, "A") + 1
        else: # 2
            start = readCheckpoint(r, "A")
    else: # 3, 4
        start = readCheckpoint(r, "A")
    
    writeCheckpoint(r, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        writeCheckpoint(r, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        if (i == N // 3):
            return
        writeCheckpoint(r, "C", value)
        # 3
        writeCheckpoint(r, "B", i)
        # 4
        client.put(key, value)

    writeCheckpoint(r, "isCompleted", 1)

def brokenFactorial3(client, key, N):
    r = redis.Redis()
    r.set_response_callback('HGET', int)

    # Recovery phase
    if (client.get(key) == readCheckpoint(r, "C")):
        if (readCheckpoint(r, "A") == readCheckpoint(r, "B")): # 1
            start = readCheckpoint(r, "A") + 1
        else: # 2
            start = readCheckpoint(r, "A")
    else: # 3, 4
        start = readCheckpoint(r, "A")
    
    writeCheckpoint(r, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        writeCheckpoint(r, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        writeCheckpoint(r, "C", value)
        # 3
        if (i == N // 3):
            return
        writeCheckpoint(r, "B", i)
        # 4
        client.put(key, value)

    writeCheckpoint(r, "isCompleted", 1)
    
def brokenFactorial4(client, key, N):
    r = redis.Redis()
    r.set_response_callback('HGET', int)

    # Recovery phase
    if (client.get(key) == readCheckpoint(r, "C")):
        if (readCheckpoint(r, "A") == readCheckpoint(r, "B")): # 1
            start = readCheckpoint(r, "A") + 1
        else: # 2
            start = readCheckpoint(r, "A")
    else: # 3, 4
        start = readCheckpoint(r, "A")
    
    writeCheckpoint(r, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        writeCheckpoint(r, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        writeCheckpoint(r, "C", value)
        # 3
        writeCheckpoint(r, "B", i)
        # 4
        if (i == N // 3):
            return
        client.put(key, value)

    writeCheckpoint(r, "isCompleted", 1)

writerFunc = client.register(writer, "writer")
readerFunc = client.register(reader, "reader")

recov = client.register(recovFactorial, "redisRecovFactorial004")
nonRecov = client.register(nonRecovFactorial, "redisNonRecovFactorial004")

borkenRecov1 = client.register(brokenFactorial1, "redisBrokenFactorial1_004")
borkenRecov2 = client.register(brokenFactorial2, "redisBrokenFactorial2_004")
borkenRecov3 = client.register(brokenFactorial3, "redisBrokenFactorial3_004")
borkenRecov4 = client.register(brokenFactorial4, "redisBrokenFactorial4_004")

def run_test(factorial, factorialStr):
    print("\n\n$$$$$$$$$$$$$$$$$$$")
    print(factorialStr)

    before = time.time()
    factorial("key", 5000).get()
    after = time.time()

    print("Seconds since epoch =", after - before)
    
def reportCheckpoints(): 
    print("isCompleted: ", readerFunc("isCompleted").get())
    print("A: ", readerFunc("A").get())
    print("B: ", readerFunc("B").get())
    print('key: {}'.format(str(readerFunc("key").get())[:5]))
    print('C: {}'.format(str(readerFunc("C").get())[:5]))
    
def clear():
    r = redis.Redis()
    r.set_response_callback('HGET', int)
    
    writerFunc("key", 1).get()
    writeCheckpoint(r, "isCompleted", 0)
    writeCheckpoint(r, "A", 1)
    writeCheckpoint(r, "B", 0)
    writeCheckpoint(r, "C", 1)
    
    print("########################\n\n")

# reportCheckpoints()
clear()

run_test(recov, "recov")
reportCheckpoints()
clear()

run_test(borkenRecov1, "borkenRecov1")
reportCheckpoints()

run_test(recov, "recov")
reportCheckpoints()
clear()

run_test(borkenRecov2, "borkenRecov2")
reportCheckpoints()

run_test(recov, "recov")
reportCheckpoints()
clear()

run_test(borkenRecov3, "borkenRecov3")
reportCheckpoints()

run_test(recov, "recov")
reportCheckpoints()
clear()

run_test(borkenRecov4, "borkenRecov4")
reportCheckpoints()

run_test(recov, "recov")
reportCheckpoints()
clear()

run_test(nonRecov, "nonRecov")
reportCheckpoints()
clear()