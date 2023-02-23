from http.server import BaseHTTPRequestHandler, HTTPServer
from subprocess import Popen
from pathlib import Path
import os,time,zmq,json,logging

class Server(BaseHTTPRequestHandler):
    # Response sending 
    def _send_response(self, status_code, message):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')                                                                              
        self.end_headers()
        self.wfile.write(json.dumps(message).encode())

    # Handling HTTP requests
    def do_GET(self):
        # Reduce calls for "/favicon.ico" that the browser automatically does
        if self.path != "/favicon.ico":
            logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Received GET Request")
            response_data = {}
            try:
                # Parsing inputted URL
                query = self.path.split('?')[1]
                query_components = dict(qc.split("=") for qc in query.split("&"))
                method = query_components.get("method", None)
                # Parsing which map reduce to use
                if method == "word-count":
                    # Parsing which data set to use
                    book = query_components.get("book", None)
                    logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Method = {method}, Book = {book}")
                    if book != None:
                        specifier = query_components.get("specifier", None)
                        data = self.wordCounter(book, specifier) 
                        if data == {'error':f'{book} was not found'}:
                            logging.error(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- {book} was not found")
                            response_data = {'error':f'{book} was not found'}
                        else:
                            response_data = {'result':data}
                    # Book was not given
                    else:
                        logging.error(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- None book provided")
                        response_data = {'error':f'No book provided'}
                # Inverted Index Handler
                elif method == "inverted-index":
                    # Ensuring a directory was given
                    directory = query_components.get("directory", None)
                    logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Method = {method}, Directory = {directory}")
                    if directory != None:
                        # Setting Default for Specifier
                        specifier = query_components.get("specifier", None)
                        data = self.invertedIndex(directory, specifier)
                        # Return response
                        response_data = {'result':data}
                        if data == {'error':f'{directory} was not found'}:
                            logging.error(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- {directory} was not found")
                            response_data = {'error':f'directory {directory} was not found'}
                    else:
                        response_data = {'error':f'No directory provided'}
                # Incorrect/Invalid Command
                else:
                    logging.exception(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- {method} IS AN INVALID COMMAND")
                    response_data = {'error':f'{method} is an invalid command'}
            # Incorrectly formed URL
            except IndexError:
                logging.exception(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- {self.path} IS INCORRECTLY FORMATED")
                response_data = {'error':'invalid command formatting'}
            except ValueError:
                logging.exception(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- {self.path} IS INCORRECTLY FORMATED")
                response_data = {'error':'invalid command formatting'}
            logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Sending Response")
            self._send_response(200, response_data)
    
    # Handles comms with the Master node for the word counter request
    def wordCounter(self, bookPath, specifier):
        request = {"method":"word count", "book": str(bookPath), "specifier":specifier}
        logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Sending request {request}...")
        socket.send_json(request)
        #  Get the reply
        message = socket.recv_json()
        logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Received reply")
        return message
    
    # Handles comms with the Master node for the invertedIndex request
    def invertedIndex(self, directory, specifier):
        request = {"method":"inverted index", "directory":directory, "specifier":specifier}
        logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Sending request {request}...")
        socket.send_json(request)
        #  Get the reply
        message = socket.recv_json()
        logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Received reply")
        return message
 
#################################################################################################################

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))

# Loading from config file
with open(SOURCE_DIR+"/config.json") as json_data_file:
    DATA = json.load(json_data_file)

# Creating Logging
logging.basicConfig(filename=SOURCE_DIR+'/logging/Server.log', level=logging.DEBUG)

# Starting master node    
master = Popen([DATA['pythonCommand'], SOURCE_DIR + "/master.py"])

#  Starting ZeroMQ
logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Connecting to ZMQ server...")
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect(f"tcp://{DATA['ip_address']}:{DATA['server']['master_zmq_port']}")

# Starting HTTP Server for API connection
hostName = DATA['ip_address']
serverPort = DATA['server']['server_port']
webServer = HTTPServer((hostName, serverPort), Server)
logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Server started http://{hostName}:{serverPort}")
try:
    webServer.serve_forever()
except KeyboardInterrupt:
    pass