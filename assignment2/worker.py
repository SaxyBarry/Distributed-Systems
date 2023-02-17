import time, random, sys, zmq, logging,os, mapreduce, json
class Worker:
    
    processID:int
    workingStatus:bool
    portNumber:int
    
    def __init__(self, processID:int) -> None:
        self.processID = processID
        self.workingStatus = False
        self.connection = "Not Established"
        # Attempting to establish connection in ZMQ with Master node
        try:
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] Creating ZMQ Connection")
            self.portNumber = str(DATA['workers']['worker_ports'][processID])
            context = zmq.Context()
            self.socket = context.socket(zmq.REP)
            self.socket.bind("tcp://*:"+self.portNumber)
            self.connection = "Established"
        except zmq.error.ZMQError:
            logging.exception(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] Unable to establish ZMQ Session, Address in Use")
            sys.exit()
    
    # Controls waiting between commands
    def awaitWork(self) -> None:
        self.workingStatus = False
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] is waiting for work")
        message = self.socket.recv_json()
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] received work")
        self.doWork(message)
    
    # Controls the workflow of the process depending on the message sent to the worker from the master
    def doWork(self, message) -> None:
        self.workingStatus = True
        method = message["method"]
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] is working:\n    Task: {message['method']}")
        if method == "map word count":   
            task = message["target"]
            self.wcmapper(task, message["hashMod"])
        elif method == "reduce word count":
            self.wcreducer()
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] is done working....")
        self.awaitWork()
        
    def wcmapper(self, task, hashMod):
        output = None
        output = mapreduce.wordCount(task)
        destination = {}
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
        logging.debug(destination.keys())
        # Sending information to reducers
        for x in range(DATA['master']['num_mappers'], DATA['master']['num_workers']):
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            socket.connect(f"tcp://{DATA['ip_address']}:{DATA['workers']['worker_ports'][x]}")
            # Reducer x gets sent destination[x - number of reducers] (3-3, 4-3, 5-3)
            if x - DATA['master']['num_reducers'] in destination.keys():
                socket.send_json(destination[x - DATA['master']['num_reducers']])
                socket.recv_json()
            else:
                socket.send_json({})
                socket.recv_json()
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] Sending reduce task {x - DATA['master']['num_reducers']} to worker {x}")
                    
    def wcreducer(self):
        self.socket.send_json({"result":"waiting"})
        # Wait for every mapper to complete their task
        x = 0
        task = {}
        while x < DATA['master']['num_mappers']:
            task[x] = self.socket.recv_json()
            self.socket.send_json({'received message':1})
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] Receiving message {x} {task[x]}")
            x += 1
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] is perfomring reduce")
        output = mapreduce.wordCountReduce(task)
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] reduce complete")
        # Waiting for master to be ready to receieve response
        message = self.socket.recv_json()
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] Received Message: {message}")
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] is sending output")
        self.socket.send_json(output)
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] output send")


if __name__ == "__main__":  
    SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Loading from config file
    with open(SOURCE_DIR+"/config.json") as json_data_file:
        DATA = json.load(json_data_file)
    
    # Creating Logging
    logging.basicConfig(filename=SOURCE_DIR+f'/logging/worker-{sys.argv[1]}.log', level=logging.DEBUG)
    worker = Worker(int(sys.argv[1]))
    worker.awaitWork()
