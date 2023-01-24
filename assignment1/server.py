import random
import socket
import os
import threading
import time
import regex

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
        return "NOT STORED\r\n"
    pattern = regex.compile("[A-z0-9]+")
    if not bool(pattern.fullmatch(key)):
        return "NOT STORED\r\n"
    # Making the key-value pair
    try:
        open(NEW_DIR+'/'+key, 'x')
        file = open(NEW_DIR+'/'+key, 'w')
        file.write(val)
        file.close()
        return "STORED\r\n"
    except:
        return "NOT STORED\r\n"


# Returns the value associated with the given key, or an appropriate status code explaining why no val was returned
def get(key: str, bytes: str):
    # Random Delay
    time.sleep(random.random())
    try:
        file = open(NEW_DIR+'/'+key, 'r')
        val = file.read()
        fileStats = os.stat(NEW_DIR+'/'+key)
        file.close()
        # Returns VALUE KEY BYTES \r\n VALUE\r\n
        return f"VALUE {key} {fileStats.st_size} \r\n{val}\r\n"
    except:
        return "KEY NOT FOUND\r\n"
    
def handleConnection(conn):
    # Receive data from Client
    data = conn.recv(1024)
    # Parsing the recieved data, splitting first by the lines, then splitting the first line to find the command that was used
    parsedData = data.decode()
    linedData = parsedData.splitlines()
    # A get or set command should consist of 2 lines at most, otherwise there are invalid newlines 
    if len(linedData) < 3:
        # Line 1 contains the command
        line1 = linedData[0].split(' ')
        if len(line1) < 4:
            # Finding the correct command
            match line1[0]:
                # Set command requires parsing the second line to get the value of the key 
                case 'set':
                    try:
                        status = set(line1[1], linedData[1], line1[2])         
                        conn.sendall(f"{status}".encode())
                    except IndexError:
                        conn.sendall("Unable to create file, invalid command structure, or loss of data due to packet size\r\n".encode())
                # Get command requires no further parsing, just finding the file if it exists
                case 'get':
                    try:
                        status = get(line1[1], line1[2])
                        # Sending the value back, and the end status message
                        conn.sendall(f"{status}END \r\n".encode())
                    except IndexError:
                        conn.sendall("Unable to read from file, invalid command structure, or loss of data due to packet size\r\n".encode())
                case _:
                    # command does not exist 
                    conn.sendall(f"INVALID COMMAND \r\n".encode())
        else:
            conn.sendall(f"Unable to process command \r\n".encode())
    else:
        conn.sendall(f"Unable to process command \r\n".encode())

if __name__ == '__main__':
    # Global variables for accessing the set storage, to be used by establishStorage, get and set commands
    SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
    NEW_DIR = os.path.join(SOURCE_DIR, "server")
    # Upon server setup, it must establish a directory for storing the values if one does not exist
    establishStorage()
    # Defining the server address
    HOST = "127.0.0.1"
    PORT = 9889
    # Bind our server to this address and listening for incoming requests.
    s = socket.socket()
    s.bind((HOST, PORT))
    s.listen(5)
    threads = []
    while True:
        conn, addr = s.accept()
        print(f"Connected by {addr}")
        client_handler = threading.Thread(target=handleConnection, args=(conn,))
        client_handler.start()

        
        
