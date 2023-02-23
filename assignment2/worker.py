import time, random, sys, zmq, logging,os, mapreduce, json    

# Controls waiting between commands
def awaitWork() -> None:
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] is waiting for work")
    message = mySocket.recv_json()
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] received work")
    doWork(message)

# Controls the workflow of the process depending on the message sent to the worker from the master
def doWork(message) -> None:
    method = message["method"]
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] is working:\n    Task: {message['method']}")
    if method == "map word count":   
        task = message["target"]
        wcmapper(task, message["hashMod"])
    elif method == "reduce word count":
        wcreducer()
    elif method == "map inverted index":
        tasks = message["target"]
        indexMapper(tasks, message["hashMod"])
    elif method == "reduce inverted index":
        indexReducer()
    # After all work is completed, wait for next job
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] is done working....")
    awaitWork()
    
def wcmapper(task, hashMod):
    # Perform Map
    output = None
    output = mapreduce.wordCount(task)
    destination = {}
    # GROUPBY
    for x in output.keys():
        # Convert each character in the word to its ASCII code 
        ascii_codes = [ord(char) for char in x]
        # Sum the ASCII codes of all characters in the word
        sum_ascii = sum(ascii_codes)
        dest = int(sum_ascii % hashMod)
        if dest in destination.keys():
            destination[dest][x] = output[x]
        else:
            destination[dest] = {}
            destination[dest][x] = output[x]
    
    # Sending information to reducers
    for x in range(DATA['master']['num_reducers']):
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{DATA['ip_address']}:{DATA['workers']['worker_ports'][x + DATA['master']['num_mappers']]}")
        if x in destination.keys():
            socket.send_json(destination[x])
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] Sending reduce task {x} to worker {DATA['workers']['worker_ports'][x + DATA['master']['num_mappers']]}")
        else:
            socket.send_json({})
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] No data exists for that reducer, Sending fake data to worker {DATA['workers']['worker_ports'][x + DATA['master']['num_mappers']]}")
        socket.recv_json()
    mySocket.send_json({"work":"complete"})
                
def wcreducer():
    mySocket.send_json({"result":"waiting"})
    # Wait for every mapper to complete their task
    x = 0
    task = {}
    while x < DATA['master']['num_mappers']:
        task[x] = mySocket.recv_json()
        mySocket.send_json({'received message':1})
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] Receiving message {x} ")
        x += 1
    # Reducing
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] is perfomring reduce")
    output = mapreduce.wordCountReduce(task)
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] reduce complete")
    # Returning the results to the master node 
    masterReceive.send_json(output)
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] output sent back to master")

def indexMapper(tasks, hashMod):
    docs = {}
    for x in tasks:
        with open(x, encoding="utf8") as book:
            docs[x] = ''.join(book.readlines())
    # Perform Map
    output = None
    output = mapreduce.mapIndex(docs)
    logging.debug(output)
    destination = {}
    # GROUPBY
    for x in output.keys():
        # Convert each character in the word to its ASCII code 
        ascii_codes = [ord(char) for char in x]
        # Sum the ASCII codes of all characters in the word
        sum_ascii = sum(ascii_codes)
        dest = int(sum_ascii % hashMod)
        if dest in destination.keys():
            destination[dest][x] = output[x]
        else:
            destination[dest] = {}
            destination[dest][x] = output[x]
    
    # Sending information to reducers
    for x in range(DATA['master']['num_reducers']):
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{DATA['ip_address']}:{DATA['workers']['worker_ports'][x + DATA['master']['num_mappers']]}")
        if x in destination.keys():
            socket.send_json(destination[x])
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] Sending reduce task {x} to worker {DATA['workers']['worker_ports'][x + DATA['master']['num_mappers']]}")
        else:
            socket.send_json({})
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] No data exists for that reducer, Sending fake data to worker {DATA['workers']['worker_ports'][x + DATA['master']['num_mappers']]}")
        socket.recv_json()
    mySocket.send_json({"work":"complete"})

# Handles reducing for inverted index
def indexReducer():
    mySocket.send_json({"result":"waiting"})
    # Wait for every mapper to complete their task
    x = 0
    task = {}
    # Receiving all data from the mappers
    while x < DATA['master']['num_mappers']:
        task[x] = mySocket.recv_json()
        mySocket.send_json({'received message':1})
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] Receiving message {x} ")
        x += 1
    # Performing Reduce
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] is perfomring reduce")
    output = mapreduce.reduceIndex(task)
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] reduce complete")
    # Sending result to the master
    masterReceive.send_json(output)
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] output sent back to master")

###############################################################################################################################

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

# Loading from config file
with open(SOURCE_DIR+"/config.json") as json_data_file:
    DATA = json.load(json_data_file)

# Creating Logging
logging.basicConfig(filename=SOURCE_DIR+f'/logging/worker-{sys.argv[1]}.log', level=logging.DEBUG)

# Attempting to establish connection in ZMQ with Master node
portNumber = sys.argv[1]

# Connecting to personal port
logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Worker] Creating ZMQ Connection")
context = zmq.Context()
mySocket = context.socket(zmq.REP)
mySocket.bind(f"tcp://*:{portNumber}")

# Connecting to the master node's receiver for results
masterReceive = context.socket(zmq.PUSH)
masterReceive.connect(f"tcp://{DATA['ip_address']}:{DATA['master']['master_receive_port']}")

# Ready for work
awaitWork()
