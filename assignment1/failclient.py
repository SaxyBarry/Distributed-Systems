# Client for expected failures!
import os
import socket
import time
import csv

# Creates a directory for storing files related to the server running: key-value storage, logs, ect...
def establishStorage():
    try:
        os.mkdir(NEW_DIR)
        print(f"Directory created at {NEW_DIR}")
    except OSError as error:
        print(f"Directory already exists at {NEW_DIR}")

def send_messages():
    # Dict of values and keys
    keyVals = {}
    
    # Specific Test Cases to test things that aren't tested with the word bank above
    keyVals['test#1'] = "This test should fail due to the special character in the name "
    keyVals['test 2'] = "This test should fail due to the space in the name "
    keyVals['test3 '] = "This test should fail due to the space in the name "
    keyVals[' test3'] = "This test should fail due to the space in the name "
    longString = 'longtest'*33
    keyVals[longString] = "This test should fail due to the length of the key"
    keyVals['longstringfail'] = "a "*1024
    
    # How many key-value pairs we have & set/gets will be made
    keyValCount = 6
    
    # Array to store data about what occurred with each command that was sent
    results = [["Key", "Value", "Bytes", "Stored?", "Time to store", "Retrieved Value", "Retrieved Bytes", "Time to get", "Correct Key-Value?"]]
    
    # Printing the results of the key-value dict
    for x in range(0, keyValCount):
        # Printing and storing the test key-values and bytes
        results.append([list(keyVals.keys())[x], keyVals[list(keyVals.keys())[x]], len(keyVals[list(keyVals.keys())[x]])])
        
    results.append(['incorrectbytesTest','This test will fail because the bytes are incorrect ' , 0])
        
    # Creating and sending the set commands for the key-values
    for x in range(0, keyValCount):
        message = f"set {list(keyVals.keys())[x]} {len(keyVals[list(keyVals.keys())[x]])} \r\n{keyVals[list(keyVals.keys())[x]]} \r\n"
        start = time.time()
        s.sendall(message.encode())
        print("Message Sent")
        data = s.recv(1024)
        end = time.time()
        print(f"{data.decode()}")
        # Storing if the key-value was stored and how long it took 
        results[x+1].extend([data.decode(), end - start])
        
    message = f"set incorrectbytesTest 0 \r\nThis test will fail because the bytes are incorrect \r\n"
    start = time.time()
    s.sendall(message.encode())
    print("Message Sent")
    data = s.recv(1024)
    end = time.time()
    print(f"{data.decode()}")
    # Storing if the key-value was stored and how long it took 
    results[keyValCount+1].extend([data.decode(), end - start])
        
    # Creating and sending the get commands for the key-values
    for x in range(0, keyValCount):
        message = f"get {list(keyVals.keys())[x]} \r\n"
        start = time.time()
        s.sendall(message.encode())
        data = s.recv(1024)
        end = time.time()
        print(f"{data.decode()}")
        linedData = data.decode().splitlines()
        line1 = linedData[0].split(' ')
        try:
            line2 = linedData[1]
        except:
            line2 = "An error occurred when parsing server return"
        # Storing the resulting return values from the store, how long the action took, and if the result and original values matched
        try:
            arr = [line2, line1[2], end - start, line2 == keyVals[list(keyVals.keys())[x]]]
        except:
            arr = [line2, "Error has Occurred", end - start, line2 == keyVals[list(keyVals.keys())[x]]]
        results[x+1].extend(arr)
        
    message = f"get incorrectbytesTest \r\n"
    start = time.time()
    s.sendall(message.encode())
    data = s.recv(1024)
    end = time.time()
    print(f"{data.decode()}")
    linedData = data.decode().splitlines()
    line1 = linedData[0].split(' ')
    try:
        line2 = linedData[1]
    except:
        line2 = "An error occurred when parsing server return"
    # Storing the resulting return values from the store, how long the action took, and if the result and original values matched
    try:
        arr = [line2, line1[2], end - start, line2 == keyVals[list(keyVals.keys())[x]]]
    except:
        arr = [line2, "Error has Occurred", end - start, line2 == keyVals[list(keyVals.keys())[x]]]
    results[keyValCount+1].extend(arr)
    return results


if __name__ == '__main__':
    SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
    NEW_DIR = os.path.join(SOURCE_DIR, "client")
    # Defining the server address
    establishStorage()
    HOST = "127.0.0.1"
    PORT = 9889
    s = socket.socket()
    s.connect((HOST, PORT))
    results = send_messages()
    s.close()
    
    # Code to store the resulting logs in a CSV file
    uni = time.time()
    file = open(NEW_DIR+'/failures'+str(uni)+'.csv', 'w')
    writer = csv.writer(file, delimiter=',')
    for x in results:
        writer.writerow(x)
    file.close()