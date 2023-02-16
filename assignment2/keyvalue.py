import json,zmq,socket,os,threading

# Creates a directory for storing files related to the server running: key-value storage, logs, ect...
def establishStorage():
    try:
        os.mkdir(NEW_DIR)
        print(f"Directory created at {NEW_DIR}")
    except OSError as error:
        print(f"Directory already exists at {NEW_DIR}")


# Stores the key-value association given, if the key has been used, returns a proper error message
def set(key: str, val: str):
    # Random Delay
    lock.acquire()
    # Making the key-value pair
    try:
        file = open(NEW_DIR+'/'+key, 'w')
        file.write(val)
        file.close()
        lock.release()
        return {'result':{'success':'stored'}}
    except:
        lock.release()
        return {'result':{'error':'not stored'}}

# Returns the value associated with the given key, or an appropriate status code explaining why no val was returned
def get(key: str):
    # Random Delay
    lock.acquire()
    try:
        file = open(NEW_DIR+'/'+key, 'r')
        val = file.read()
        file.close()
        fileStats = os.stat(NEW_DIR+'/'+key)
        # Returns VALUE KEY BYTES \r\n VALUE\r\n
        lock.release()
        return {'result':{'value':val}}
    except:
        lock.release()
        return {'result':{'error':'not stored'}}
    
def handleRequest(message:dict):
    method = message['method']
    key = message['key']
    if method == 'set':
        # Set command requires parsing the second line to get the value of the key 
            try:
                value = message['value']
                status = set(key, value)  
                socket.send_json(status)
            except IndexError:
                socket.send_json({"error":"not stored"})
        # Get command requires no further parsing, just finding the file if it exists
    elif method == 'get':
            status = get(key)
            # Sending the value back, and the end status message
            socket.send_json(status)
    else:
        socket.send_json({'result':{"error":"not stored"}})
        

if __name__ == '__main__':
    # Global variables for accessing the set storage, to be used by establishStorage, get and set commands
    SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
    NEW_DIR = os.path.join(SOURCE_DIR, "keyvalue")
    with open("config.json") as json_data_file:
        DATA = json.load(json_data_file)
    # Mutex Lock to Prevent multiple thread locks at once
    lock = threading.Lock()
    # Upon server setup, it must establish a directory for storing the values if one does not exist
    establishStorage()
    # Defining the server address
    HOST = DATA['ip_address']
    PORT = DATA['keyvalue']['port']
    # Bind our server to this address and listening for incoming requests.
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:"+str(PORT))
    while True:
        message = socket.recv_json()
        handleRequest(message)
        
