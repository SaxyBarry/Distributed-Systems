import random
import socket
import os
import threading
import time
import re

# Creates a directory for storing files related to the server running: key-value storage, logs, ect...
def establishStorage():
    try:
        os.mkdir(NEW_DIR)
        print(f"Directory created at {NEW_DIR}")
    except OSError as error:
        print(f"Directory already exists at {NEW_DIR}")


# Stores the key-value association given, if the key has been used, returns a proper error message
def set(key: str, val: str, bytes: str):
    # Random Delay
    time.sleep(random.random()) 
    # Checking for valid key: Less than 260 characters, no special characters/whitespace
    if len(key) >= 260: 
        return "NOT STORED \r\n"
    if not (len(val) == int(bytes)):
        return "NOT STORED \r\n"
    pattern = re.compile("[A-z0-9]+")
    if not bool(pattern.fullmatch(key)):
        return "NOT STORED \r\n"
    lock.acquire()
    # Making the key-value pair
    try:
        file = open(NEW_DIR+'/'+key, 'w')
        file.write(val)
        file.close()
        lock.release()
        return "STORED \r\n"
    except:
        lock.release()
        return "NOT STORED \r\n"

# Returns the value associated with the given key, or an appropriate status code explaining why no val was returned
def get(key: str, bytes: str):
    # Random Delay
    time.sleep(random.random())
    lock.acquire()
    try:
        file = open(NEW_DIR+'/'+key, 'r')
        val = file.read()
        file.close()
        fileStats = os.stat(NEW_DIR+'/'+key)
        # Returns VALUE KEY BYTES \r\n VALUE\r\n
        lock.release()
        return f"VALUE {key} {fileStats.st_size} \r\n{val}\r\n"
    except:
        lock.release()
        return "NOT STORED \r\n"
    
def handleRequest(parsedData, conn):
    linedData = parsedData.splitlines()
    # A get or set command should consist of 2 lines at most, otherwise there are invalid newlines 
    if len(linedData) < 4:
        # Line 1 contains the command
        line1 = linedData[0].split(' ')
        if len(line1) < 5:
            # Finding the correct command
            if line1[0] == 'set':
                # Set command requires parsing the second line to get the value of the key 
                    try:
                        status = set(line1[1], linedData[1], line1[2])  
                        conn.sendall(f"{status}".encode())
                    except IndexError:
                        conn.sendall("NOT STORED \r\n".encode())
                # Get command requires no further parsing, just finding the file if it exists
            elif line1[0] == 'get':
                    try:
                        status = get(line1[1], line1[2])
                        # Sending the value back, and the end status message
                        if status == "NOT STORED \r\n":
                            conn.sendall(status.encode())
                        else:
                            conn.sendall(f"{status}END \r\n".encode())
                    except IndexError:
                        conn.sendall("NOT STORED \r\n".encode())
            else:
                # command does not exist 
                conn.sendall(f"NOT STORED \r\n".encode())
        else:
            conn.sendall(f"NOT STORED \r\n".encode())
    else:
        conn.sendall(f"NOT STORED \r\n".encode())
    
def handleConnection(conn):
    while True:
        data = conn.recv(1024)
        if not data:
            break
        # Parsing the recieved data, splitting first by the lines, then splitting the first line to find the command that was used
        parsedData = data.decode()
        thread = threading.Thread(target=handleRequest, args=(parsedData,conn,))
        thread.start()
    conn.close()
        

if __name__ == '__main__':
    # Global variables for accessing the set storage, to be used by establishStorage, get and set commands
    SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
    NEW_DIR = os.path.join(SOURCE_DIR, "server")
    # Mutex Lock to Prevent multiple thread locks at once
    lock = threading.Lock()
    # Upon server setup, it must establish a directory for storing the values if one does not exist
    establishStorage()
    # Defining the server address
    HOST = "127.0.0.1"
    PORT = 9179
    # Bind our server to this address and listening for incoming requests.
    s = socket.socket()
    s.bind((HOST, PORT))
    s.listen()
    while True:
        conn, addr = s.accept()
        client_thread = threading.Thread(target = handleConnection, args = (conn,))
        client_thread.start()
