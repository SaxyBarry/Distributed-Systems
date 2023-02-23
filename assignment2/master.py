from subprocess import Popen
from pathlib import Path
import os,time,zmq,json,sys, logging, uuid, math

# Creates Worker Processes
def establishWorkers():
    mappers = {}
    reducers = {}
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Establishing Workers")
    # Creating the mappers
    for x in range(DATA['master']['num_mappers']):
        mappers[x] = {}
        mappers[x]['process'] = Popen([DATA['pythonCommand'], SOURCE_DIR + "/worker.py", str(DATA['workers']['worker_ports'][x])])
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{DATA['ip_address']}:{DATA['workers']['worker_ports'][x]}")
        mappers[x]['socket'] = socket
    # Creating Reducers
    for x in range(DATA['master']['num_mappers'], DATA['master']['num_workers']):
        reducers[x] = {}
        reducers[x]['process'] = Popen([DATA['pythonCommand'], SOURCE_DIR + "/worker.py", str(DATA['workers']['worker_ports'][x])])
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{DATA['ip_address']}:{DATA['workers']['worker_ports'][x]}")
        reducers[x]['socket'] = socket
    return mappers, reducers

# Controls waiting & receiving requests
def awaitWork():
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Waiting for work....")
        message = frontend.recv_json()
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Received Message: {str(message)}")
        method = message["method"]
        if method == "word count":
            wordCount(message["book"], message["specifier"])
        if method == "inverted index":
            invertedIndex(message["directory"], message["specifier"])

def wordCount(book, specifier):
    # Step 1. Find file that is being requested for word count
        # If that data set exists, then begin the word count on it
        bookPath = Path(SOURCE_DIR+"/books/"+book)
        if bookPath.is_file():
            fileLines = []
            with open(bookPath, encoding="utf8") as file:
                logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] file found and opened")
                fileStats = os.stat(bookPath)
                fileLines = file.readlines()
    # Step 2. Tell reducers to prepare to receive data
            for x in reducers.keys():
                reducers[x]['socket'].send_json({"method":"reduce word count"})
                message = reducers[x]['socket'].recv_json()
                logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Received Message from worker {x}: {message}")
    # Step 3. Divide mapping work between mappers 
            partition = math.ceil(len(fileLines) / DATA['master']['num_mappers'])
            for x in mappers.keys():
                text = ""
                for y in range(partition):
                    if fileLines != []:
                        text += str(fileLines.pop(0))
                logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Sending worker {x} new map task")
                # Tell worker to do the work
                mappers[x]['socket'].send_json({"method":"map word count", "target":text, 'hashMod': DATA["master"]['num_reducers']})
    # Step 4. Wait for mappers to finish
            for x in mappers.keys():
                logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Receiving Acknowledgement from Worker {x}")
                mappers[x]['socket'].recv_json()
    # Step 5. Wait for reducers to finish work 
            reduceResult = {}
            response = {}
            # Receive Worker Acknowledgement
            for x in reducers.keys():
                logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Receiving message from worker {x}")
                reduceResult[x] = receiving.recv_json()
            if specifier != None:
                response = {specifier:0}
                for x in reducers.keys():
                    if specifier in reduceResult[x].keys():
                        response = {specifier:reduceResult[x][specifier]}
                respond(response)
            else:
                response = {}
                for x in reducers.keys():
                    response.update(reduceResult[x])
        else:
            response = {'error':f'{book} was not found'}
    # Step 6. Tell API user the work is completed
        respond(response)

def invertedIndex(directory, specifier):
  # Step 1. Get all of books in the given directory 
    files = []
    # Iterate directory
    if os.path.isdir(f'{SOURCE_DIR}/{directory}/'):
        for path in os.listdir(f'{SOURCE_DIR}/{directory}/'):
            # check if current path is a file
            if os.path.isfile(os.path.join(f'{SOURCE_DIR}/{directory}/', path)):
                files.append(os.path.join(f'{SOURCE_DIR}/{directory}/', path))
        
        # Step 2. Tell reducers to prepare to receive data
        for x in reducers.keys():
            reducers[x]['socket'].send_json({"method":"reduce inverted index"})
            message = reducers[x]['socket'].recv_json()
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Received Message from worker {x}: {message}")
            
        # Step 3. Divide mapping work between mappers 
        # Here we divide the number of files by the number of mappers in the given directory
        partition = math.ceil(len(files) / DATA['master']['num_mappers'])
        for x in mappers.keys():
            books = []
            for y in range(partition):
                if len(files) > y:
                    books.append(files.pop(y))
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Sending worker {x} new map task")
            # Tell worker to do the work
            mappers[x]['socket'].send_json({"method":"map inverted index", "target":books, 'hashMod': DATA["master"]['num_reducers']})
            
        # Step 4. Wait for mappers to finish
        for x in mappers.keys():
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Receiving Acknowledgement from Worker {x}")
            mappers[x]['socket'].recv_json()
        # Step 5. Wait for reducers to finish work 
        reduceResult = {}
        response = {}
        # Receive Worker Acknowledgement
        for x in reducers.keys():
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Receiving message from worker {x}")
            reduceResult[x] = receiving.recv_json()
        # If there is a specifier, filter the results by the given specifier
        if specifier != None:
            response = {specifier:[]}
            for x in reducers.keys():
                if specifier in reduceResult[x].keys():
                    response = {specifier:reduceResult[x][specifier]}
            respond(response)
        else:
            response = {}
            for x in reducers.keys():
                response.update(reduceResult[x])
    else:
        response = {'error':f'{directory} was not found'}
    # Step 6. Tell API user the work is completed
    respond(response)

# Sends messages back to the server         
def respond(message):
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Responding to Server")
    frontend.send_json(message)
    awaitWork()

##################################################################################################################################
SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

# Loading from config file
with open(SOURCE_DIR+"/config.json") as json_data_file:
    DATA = json.load(json_data_file)

# Establishing logging 
logging.basicConfig(filename=SOURCE_DIR+'/logging/master.log', level=logging.DEBUG)
logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Starting Master Node")

# Establishing ZMQ Connection with Server
logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Establishing ZMQ Session with Server")

# Connection to server
context = zmq.Context()
frontend = context.socket(zmq.REP)
frontend.bind(f"tcp://*:{DATA['server']['master_zmq_port']}")
# Node to receive results
receiving = context.socket(zmq.PULL)
receiving.bind(f"tcp://*:{DATA['master']['master_receive_port']}")

# Creating Mappers and reducers
mappers, reducers = establishWorkers()

# Ready to work
awaitWork()
