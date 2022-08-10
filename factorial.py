
from cloudburst.client.client import CloudburstConnection
import time

ip = "localhost"
client = CloudburstConnection(ip, ip, local=True)

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
    # Recovery phase
    if ((client.get(key) == client.get("C")) and client.get("A") == client.get("B")):
        start = client.get("A") + 1
    else: # 2, 3, 4
        start = client.get("A")
    
    client.put("isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        client.put("A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        client.put("C", value)
        # 3
        client.put("B", i)
        # 4
        client.put(key, value)

    client.put("isCompleted", 1)

def brokenFactorial1(client, key, N):
    # Recovery phase
    if (client.get(key) == client.get("C")):
        if (client.get("A") == client.get("B")): # 1
            start = client.get("A") + 1
        else: # 2
            start = client.get("A")
    else: # 3, 4
        start = client.get("A")
    
    client.put("isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        if (i == N // 3):
            return
        
        client.put("A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        client.put("C", value)
        # 3
        client.put("B", i)
        # 4
        client.put(key, value)

    client.put("isCompleted", 1)
    
def brokenFactorial2(client, key, N):
    # Recovery phase
    if (client.get(key) == client.get("C")):
        if (client.get("A") == client.get("B")): # 1
            start = client.get("A") + 1
        else: # 2
            start = client.get("A")
    else: # 3, 4
        start = client.get("A")
    
    client.put("isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        client.put("A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        if (i == N // 3):
            return
        client.put("C", value)
        # 3
        client.put("B", i)
        # 4
        client.put(key, value)

    client.put("isCompleted", 1)

def brokenFactorial3(client, key, N):
    # Recovery phase
    if (client.get(key) == client.get("C")):
        if (client.get("A") == client.get("B")): # 1
            start = client.get("A") + 1
        else: # 2
            start = client.get("A")
    else: # 3, 4
        start = client.get("A")
    
    client.put("isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        client.put("A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        client.put("C", value)
        # 3
        if (i == N // 3):
            return
        client.put("B", i)
        # 4
        client.put(key, value)

    client.put("isCompleted", 1)
    
def brokenFactorial4(client, key, N):
    # Recovery phase
    if (client.get(key) == client.get("C")):
        if (client.get("A") == client.get("B")): # 1
            start = client.get("A") + 1
        else: # 2
            start = client.get("A")
    else: # 3, 4
        start = client.get("A")
    
    client.put("isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        client.put("A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        client.put("C", value)
        # 3
        client.put("B", i)
        # 4
        if (i == N // 3):
            return
        client.put(key, value)

    client.put("isCompleted", 1)

writerFunc = client.register(writer, "writer")
readerFunc = client.register(reader, "reader")

recov = client.register(recovFactorial, "recovFactorial003")
nonRecov = client.register(nonRecovFactorial, "nonRecovFactorial003")

borkenRecov1 = client.register(brokenFactorial1, "brokenFactorial1_003")
borkenRecov2 = client.register(brokenFactorial2, "brokenFactorial2_003")
borkenRecov3 = client.register(brokenFactorial3, "brokenFactorial3_003")
borkenRecov4 = client.register(brokenFactorial4, "brokenFactorial4_003")

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
    writerFunc("isCompleted", 0).get()
    writerFunc("key", 1).get()
    writerFunc("A", 1).get()
    writerFunc("B", 0).get()
    writerFunc("C", 1).get()
    
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