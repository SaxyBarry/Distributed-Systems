from subprocess import Popen
from pathlib import Path
import os,time,zmq,json,sys, logging, uuid, math
class Master:
    workers = {}
    def __init__(self, apiPort):
        # Establishing ZMQ Connection with Server
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Establishing ZMQ Session with Server")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:"+apiPort)

    # Creates Worker Processes
    def establishWorkers(self):
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Establishing Workers")
        for x in range(DATA['master']['num_workers']):
            # Stores workers in dictionary
            self.workers[x] = {}
            self.workers[x]['process'] = Popen(['python', SOURCE_DIR + "/worker.py", str(x)])
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Establishing ZMQ Session with Worker {x}")
            # Setting up ZMQ Connection with worker
            socket = self.context.socket(zmq.REQ)
            self.workers[x]['socket'] = socket
            socket.connect(f"tcp://{DATA['ip_address']}:{DATA['workers']['worker_ports'][x]}")

    # Controls waiting & receiving requests
    def awaitWork(self):
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Waiting for work....")
            message = self.socket.recv_json()
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Received Message: {str(message)}")
            method = message["method"]
            if method == "word count":
                self.wordCount(message["book"], message["specifier"])
    
    def wordCount(self, book, specifier):
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
                for x in range(DATA['master']['num_mappers'], DATA['master']['num_workers']):
                    self.workers[x]['socket'].send_json({"method":"reduce word count"})
                    message = self.workers[x]['socket'].recv_json()
                    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Received Message from worker {x}: {message}")
        # Step 3. Divide mapping work between mappers 
                partition = math.ceil(len(fileLines) / DATA['master']['num_mappers'])
                for x in range(DATA['master']['num_mappers']):
                    text = ""
                    for y in range(partition):
                        if fileLines != []:
                            text += str(fileLines.pop(0))
                    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Sending worker {x} new map task")
                    # Tell worker to do the work
                    self.workers[x]['socket'].send_json({"method":"map word count", "target":text, 'hashMod': DATA["master"]['num_reducers']})
                    
        # Step 5. Wait for reducers to finish work 
                reduceResult = {}
                response = {}
                # Receive Worker Acknowledgement
                for x in range(DATA['master']['num_mappers'], DATA['master']['num_workers']):
                    reduceResult[x] = self.workers[x]['socket'].send_json({})
                    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Receiving message from worker {x}")
                    reduceResult[x] = self.workers[x]['socket'].recv_json()
                if specifier != None:
                    response = {specifier:0}
                    for x in range(DATA['master']['num_reducers']):
                        if specifier in reduceResult[x].keys():
                            response = {specifier:reduceResult[x][specifier]}
                    self.respond(response)
                else:
                    response = {}
                    for x in range(DATA['master']['num_reducers']):
                        response.update(reduceResult[x])
            else:
                response = {'error':f'{book} was not found'}
        # Step 6. Tell API user the work is completed
            self.respond(response)
    

   # Sends messages back to the server         
    def respond(self, message):
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Responding to Server")
        self.socket.send_json(message)
        self.awaitWork()

if __name__ == "__main__": 
    SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Loading from config file
    with open(SOURCE_DIR+"/config.json") as json_data_file:
        DATA = json.load(json_data_file)
    
    
    # Establishing logging 
    logging.basicConfig(filename=SOURCE_DIR+'/logging/master.log', level=logging.DEBUG)
    logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [Master] Starting Master Node")
    
    # Creating master that opens a ZMQ connection on the command line passed arg
    master = Master(sys.argv[1])
    master.establishWorkers()
    master.awaitWork()
