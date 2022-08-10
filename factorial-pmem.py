
from ast import Try
from asyncore import write
from cloudburst.client.client import CloudburstConnection
import time

ip = "localhost"
client = CloudburstConnection(ip, ip, local=True)

def clearFunc(client):
    client.put("key", 1)
    client.pmem_put_A(1)
    client.pmem_put_B(0)
    client.pmem_put_C(1)
    
def reportCheckpointsFunc(client):
    result = ""
    result += "A: {}\n".format(client.pmem_get_A())
    result += "B: {}\n".format(client.pmem_get_B())
    result += 'key: {}\n'.format(str(client.get("key"))[:5])
    result += 'C: {}\n'.format(str(client.pmem_get_C())[:5])
    
    return result

def recovFactorial(client, key, N):
    # Recovery phase
    if ((client.get(key) == client.pmem_get_C())\
            and client.pmem_get_A() == client.pmem_get_B()):
        
        start = client.pmem_get_A() + 1
    else: # 2, 3, 4
        start = client.pmem_get_A()
    
    # Execution
    for i in range(start, N + 1):
        # 1
        client.pmem_put_A(i)
        
        value = client.get(key)
        value *= i
        
        # 2
        client.pmem_put_C(value)
        # 3
        client.pmem_put_B(i)
        # 4
        client.put(key, value)


def brokenFactorial1(client, key, N):
    # Recovery phase
    if (client.get(key) == client.pmem_get_C()):
        if (client.pmem_get_A() == client.pmem_get_B()): # 1
            start = client.pmem_get_A() + 1
        else: # 2
            start = client.pmem_get_A()
    else: # 3, 4
        start = client.pmem_get_A()
    
    # Execution
    for i in range(start, N + 1):
        # 1
        if (i == N // 3):
            return
        
        client.pmem_put_A(i)
        
        value = client.get(key)
        value *= i
        
        # 2
        client.pmem_put_C(value)
        # 3
        client.pmem_put_B(i)
        # 4
        client.put(key, value)

    
def brokenFactorial2(client, key, N):
    # Recovery phase
    if (client.get(key) == client.pmem_get_C()):
        if (client.pmem_get_A() == client.pmem_get_B()): # 1
            start = client.pmem_get_A() + 1
        else: # 2
            start = client.pmem_get_A()
    else: # 3, 4
        start = client.pmem_get_A()
    
    # Execution
    for i in range(start, N + 1):
        # 1
        client.pmem_put_A(i)
        
        value = client.get(key)
        value *= i
        
        # 2
        if (i == N // 3):
            return
        client.pmem_put_C(value)
        # 3
        client.pmem_put_B(i)
        # 4
        client.put(key, value)


def brokenFactorial3(client, key, N):
    # Recovery phase
    if (client.get(key) == client.pmem_get_C()):
        if (client.pmem_get_A() == client.pmem_get_B()): # 1
            start = client.pmem_get_A() + 1
        else: # 2
            start = client.pmem_get_A()
    else: # 3, 4
        start = client.pmem_get_A()
    
    # Execution
    for i in range(start, N + 1):
        # 1
        client.pmem_put_A(i)
        
        value = client.get(key)
        value *= i
        
        # 2
        client.pmem_put_C(value)
        # 3
        if (i == N // 3):
            return
        client.pmem_put_B(i)
        # 4
        client.put(key, value)

    
def brokenFactorial4(client, key, N):
    # Recovery phase
    if (client.get(key) == client.pmem_get_C()):
        if (client.pmem_get_A() == client.pmem_get_B()): # 1
            start = client.pmem_get_A() + 1
        else: # 2
            start = client.pmem_get_A()
    else: # 3, 4
        start = client.pmem_get_A()
    
    # Execution
    for i in range(start, N + 1):
        # 1
        client.pmem_put_A(i)
        
        value = client.get(key)
        value *= i
        
        # 2
        client.pmem_put_C(value)
        # 3
        client.pmem_put_B(i)
        # 4
        if (i == N // 3):
            return
        client.put(key, value)


def nonRecovFactorial(client, key, N):
    for i in range(1, N + 1):
        value = client.get(key)
        value *= i
        client.put(key, value)    


clear = client.register(clearFunc, "clearFunc011")
reportCheckpoints = client.register(reportCheckpointsFunc, "reportCheckpointsFunc011")

recov = client.register(recovFactorial, "recovFactorial011")
nonRecov = client.register(nonRecovFactorial, "rnonRecovFactorial011")

borkenRecov1 = client.register(brokenFactorial1, "brokenFactorial1_011")
borkenRecov2 = client.register(brokenFactorial2, "brokenFactorial2_011")
borkenRecov3 = client.register(brokenFactorial3, "brokenFactorial3_011")
borkenRecov4 = client.register(brokenFactorial4, "brokenFactorial4_011")

def run_test(factorial, factorialStr):
    print("\n\n$$$$$$$$$$$$$$$$$$$")
    print(factorialStr)

    before = time.time()
    factorial("key", 100).get()
    after = time.time()

    print("Seconds since epoch =", after - before)    

print(reportCheckpoints().get())
clear().get()

run_test(recov, "recov")
print(reportCheckpoints().get())
clear().get()

run_test(borkenRecov1, "borkenRecov1")
print(reportCheckpoints().get())

run_test(recov, "recov")
print(reportCheckpoints().get())
clear().get()

run_test(borkenRecov2, "borkenRecov2")
print(reportCheckpoints().get())

run_test(recov, "recov")
print(reportCheckpoints().get())

run_test(recov, "recov")
print(reportCheckpoints().get())

clear().get()

run_test(borkenRecov3, "borkenRecov3")
print(reportCheckpoints().get())

run_test(recov, "recov")
print(reportCheckpoints().get())
clear().get()

run_test(borkenRecov4, "borkenRecov4")
print(reportCheckpoints().get())

run_test(recov, "recov")
print(reportCheckpoints().get())
clear().get()

run_test(nonRecov, "nonRecov")
print(reportCheckpoints().get())
clear().get()