# This client should be enough to cover the test cases:
# It utilizes a word bank, and creates keys and messages of random sizes for a random number of connections
# It then sends an equal amount of get/set commands to set the key-value, then to retrieve the values set previously
# There is no collision resistence in this client other than the randomness, so it is possible for keys/messages to be duplicated across running
# This demonstrates an interesting test case where a key is duplicated, which displays the server's ability to handle duplicates in a unique system 
# For logs about runs of the client system, please look at the client folder in this directory. It contains CSVs that display the correctness and speed of the commands to the server
import os
import random
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
    wordBank = ["hello", 'world', 'how', 'are', 'you', "monitor", "program", "application", "keyboard", 
                "javascript", "gaming", "network",'evaporate','let','willing','zipper','voyage',
                'imprint','space','force','credit','present','messy','build','shape','crowded','exuberant',
                'suit','jelly','limping','whisper','1','2','3','4','5','6','7','8','9','0']
    # Dict of values and keys
    keyVals = {}
    
    # Specific Test Cases to test things that aren't tested with the word bank above
    keyVals['test#1'] = "This test should fail due to the special character in the name "
    keyVals['test 2'] = "This test should fail due to the space in the name "
    keyVals['test3 '] = "This test should fail due to the space in the name "
    keyVals['test5'] = f"This test is a duplicate key {random.randint(0,15)} "
    longString = 'longtest'*33
    keyVals[longString] = "This test should fail due to the length of the string"
    
    # How many key-value pairs we have & set/gets will be made
    keyValCount = random.randint(1, 15) + 5
    
    # Populating dictionary with keys and values, keys are composed of 1-3 words from above, values are composed of 1-10 words from above 
    for x in range(0, keyValCount):
        key = ""
        for y in range(0, random.randint(1, 3)):
            key += random.choice(wordBank)
        keyVals[key] = ""
        for y in range(0, random.randint(1, 15)):
            keyVals[key] += random.choice(wordBank) + ' '
    # Array to store data about what occurred with each command that was sent
    results = [["Key", "Value", "Bytes", "Stored?", "Time to store", "Retrieved Value", "Retrieved Bytes", "Time to get", "Correct Key-Value?"]]
    
    # Printing the results of the key-value dict
    for x in range(0, keyValCount):
        # Printing and storing the test key-values and bytes
        results.append([list(keyVals.keys())[x], keyVals[list(keyVals.keys())[x]], len(keyVals[list(keyVals.keys())[x]])])
        
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
    open(NEW_DIR+'/log'+str(uni)+'.csv', 'x')
    file = open(NEW_DIR+'/log'+str(uni)+'.csv', 'w')
    writer = csv.writer(file, delimiter=',')
    for x in results:
        writer.writerow(x)
    file.close()