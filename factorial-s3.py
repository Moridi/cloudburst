import boto3
from cloudburst.client.client import CloudburstConnection
import time

ACCESS_KEY = "AKIA3WXB7KRB7GQNRL7F"
SECRET_KEY = "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"
BUCKET_NAME = "myserverlessbucket12"

ip = "localhost"
client = CloudburstConnection(ip, ip, local=True)

def readCheckpointS3(s3_client, field): 
    return int(s3_client.get_object(Bucket=BUCKET_NAME, Key=field)['Body'].read().decode())

def writeCheckpointS3(s3_client, field, value): 
    s3_client.put_object(Bucket=BUCKET_NAME, Key=field, Body=f"{value}")

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
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    # Recovery phase
    if ((client.get(key) == readCheckpointS3(s3_client, "C")) and readCheckpointS3(s3_client, "A") == readCheckpointS3(s3_client, "B")):
        start = readCheckpointS3(s3_client, "A") + 1
    else: # 2, 3, 4
        start = readCheckpointS3(s3_client, "A")

    writeCheckpointS3(s3_client, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        writeCheckpointS3(s3_client, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        writeCheckpointS3(s3_client, "C", value)
        # 3
        writeCheckpointS3(s3_client, "B", i)
        # 4
        client.put(key, value)
    
    writeCheckpointS3(s3_client, "A", 2555)

    writeCheckpointS3(s3_client, "isCompleted", 1)

def brokenFactorial1(client, key, N):
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    # Recovery phase
    if (client.get(key) == readCheckpointS3(s3_client, "C")):
        if (readCheckpointS3(s3_client, "A") == readCheckpointS3(s3_client, "B")): # 1
            start = readCheckpointS3(s3_client, "A") + 1
        else: # 2
            start = readCheckpointS3(s3_client, "A")
    else: # 3, 4
        start = readCheckpointS3(s3_client, "A")
    
    writeCheckpointS3(s3_client, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        if (i == N // 3):
            return
        
        writeCheckpointS3(s3_client, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        writeCheckpointS3(s3_client, "C", value)
        # 3
        writeCheckpointS3(s3_client, "B", i)
        # 4
        client.put(key, value)

    writeCheckpointS3(s3_client, "isCompleted", 1)
    
def brokenFactorial2(client, key, N):
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    # Recovery phase
    if (client.get(key) == readCheckpointS3(s3_client, "C")):
        if (readCheckpointS3(s3_client, "A") == readCheckpointS3(s3_client, "B")): # 1
            start = readCheckpointS3(s3_client, "A") + 1
        else: # 2
            start = readCheckpointS3(s3_client, "A")
    else: # 3, 4
        start = readCheckpointS3(s3_client, "A")
    
    writeCheckpointS3(s3_client, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        writeCheckpointS3(s3_client, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        if (i == N // 3):
            return
        writeCheckpointS3(s3_client, "C", value)
        # 3
        writeCheckpointS3(s3_client, "B", i)
        # 4
        client.put(key, value)

    writeCheckpointS3(s3_client, "isCompleted", 1)

def brokenFactorial3(client, key, N):
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    # Recovery phase
    if (client.get(key) == readCheckpointS3(s3_client, "C")):
        if (readCheckpointS3(s3_client, "A") == readCheckpointS3(s3_client, "B")): # 1
            start = readCheckpointS3(s3_client, "A") + 1
        else: # 2
            start = readCheckpointS3(s3_client, "A")
    else: # 3, 4
        start = readCheckpointS3(s3_client, "A")
    
    writeCheckpointS3(s3_client, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        writeCheckpointS3(s3_client, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        writeCheckpointS3(s3_client, "C", value)
        # 3
        if (i == N // 3):
            return
        writeCheckpointS3(s3_client, "B", i)
        # 4
        client.put(key, value)

    writeCheckpointS3(s3_client, "isCompleted", 1)
    
def brokenFactorial4(client, key, N):
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    # Recovery phase
    if (client.get(key) == readCheckpointS3(s3_client, "C")):
        if (readCheckpointS3(s3_client, "A") == readCheckpointS3(s3_client, "B")): # 1
            start = readCheckpointS3(s3_client, "A") + 1
        else: # 2
            start = readCheckpointS3(s3_client, "A")
    else: # 3, 4
        start = readCheckpointS3(s3_client, "A")
    
    writeCheckpointS3(s3_client, "isCompleted", 0)
    # Execution
    for i in range(start, N + 1):
        # 1
        writeCheckpointS3(s3_client, "A", i)
        
        value = client.get(key)
        value *= i
        
        # 2
        writeCheckpointS3(s3_client, "C", value)
        # 3
        writeCheckpointS3(s3_client, "B", i)
        # 4
        if (i == N // 3):
            return
        client.put(key, value)

    writeCheckpointS3(s3_client, "isCompleted", 1)

writerFunc = client.register(writer, "writer")
readerFunc = client.register(reader, "reader")

recov = client.register(recovFactorial, "S3RecovFactorial009")
nonRecov = client.register(nonRecovFactorial, "S3NonRecovFactorial009")

# borkenRecov1 = client.register(brokenFactorial1, "S3BrokenFactorial1_009")
# borkenRecov2 = client.register(brokenFactorial2, "S3BrokenFactorial2_009")
# borkenRecov3 = client.register(brokenFactorial3, "S3BrokenFactorial3_009")
# borkenRecov4 = client.register(brokenFactorial4, "S3BrokenFactorial4_009")

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
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    writerFunc("key", 1).get()
    writeCheckpointS3(s3_client, "isCompleted", 0)
    writeCheckpointS3(s3_client, "A", 1)
    writeCheckpointS3(s3_client, "B", 0)
    writeCheckpointS3(s3_client, "C", 1)
    
    print("########################\n\n")

# reportCheckpoints()
clear()

run_test(recov, "recov")
reportCheckpoints()
clear()

# run_test(borkenRecov1, "borkenRecov1")
# reportCheckpoints()

# run_test(recov, "recov")
# reportCheckpoints()
# clear()

# run_test(borkenRecov2, "borkenRecov2")
# reportCheckpoints()

# run_test(recov, "recov")
# reportCheckpoints()
# clear()

# run_test(borkenRecov3, "borkenRecov3")
# reportCheckpoints()

# run_test(recov, "recov")
# reportCheckpoints()
# clear()

# run_test(borkenRecov4, "borkenRecov4")
# reportCheckpoints()

# run_test(recov, "recov")
# reportCheckpoints()
# clear()

# run_test(nonRecov, "nonRecov")
# reportCheckpoints()
# clear()