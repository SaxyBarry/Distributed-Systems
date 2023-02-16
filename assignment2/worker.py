import time, random, sys, zmq, logging,os, mapreduce, json
class Worker:
    
    processID:int
    workingStatus:bool
    portNumber:int
    
    def __init__(self, processID:int) -> None:
        self.processID = processID
        self.workingStatus = False
        self.connection = "Not Established"
        
    # MAYBE? Intermediate Storage Implementation
        # logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] Creating personal directory")
        # # Creating directory for intermediate results to be stored in
        # try:
        #     os.mkdir(SOURCE_DIR + f"\\common\\{processID}")
        # except FileExistsError:
        #     logging.exception(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] Directory already created")
        
        # Attempting to establish connection in ZMQ with Master node
        try:
            logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] Creating ZMQ Connection")
            self.portNumber = str(DATA['workers']['starting_port']+processID)
            context = zmq.Context()
            self.socket = context.socket(zmq.REP)
            self.socket.bind("tcp://*:"+self.portNumber)
            self.connection = "Established"
        # Sometimes the port that is assigned is taken, which will cause a failure
        # **NEED BETTER WAY TO CREATE NEW PROCESS FROM MASTER SO THIS ERROR IS HANDLED**
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
        task = message["target"]
        method = message["method"]
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] is working:\n    Task: {message['method']}")
        if method == "map word count":   
            result = self.wcmapper(task, message["hashMod"])
        elif method == "reduce word count":
            result = self.wcreducer(task)
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] is done working....")
        self.respond(result)
        
    def wcmapper(self, task, hashMod):
        output = None
        # with open(task, encoding="utf8") as file: 
        #     output = mapreduce.wordCountReduce(file.read(-1))
        output = mapreduce.wordCount(task)
        logging.debug(output)
        destination = {}
        for x in output.keys():
            # Convert each character in the word to its ASCII code 
            ascii_codes = [ord(char) for char in x]

            # Sum the ASCII codes of all characters in the word
            sum_ascii = sum(ascii_codes)
            dest = sum_ascii % hashMod
            if dest in destination.keys():
                destination[dest][x] = output[x]
            else:
                destination[dest] = {}
                destination[dest][x] = output[x]
                
        # for x in destination.keys():
        #     with open(f'{SOURCE_DIR }\\common\\{self.processID}\\{x}.txt', 'w') as file:
        #         for y in destination[x].keys():
        #             file.write(f'{y},{destination[x][y]}\n')
        return destination
                    
    def wcreducer(self, task):
        output = mapreduce.wordCountReduce(task)
        return output

    def respond(self, result) -> None:
        logging.debug(f"  {time.strftime('%H:%M:%S', time.localtime())} [{self.processID}] is responding")
        self.socket.send_json(result)
        self.awaitWork()

if __name__ == "__main__":  
    SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Loading from config file
    with open("config.json") as json_data_file:
        DATA = json.load(json_data_file)
    
    # Creating Logging
    logging.basicConfig(filename=SOURCE_DIR+f'/logging/worker-{sys.argv[1]}.log', level=logging.DEBUG)
    worker = Worker(int(sys.argv[1]))
    worker.awaitWork()
