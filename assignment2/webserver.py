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
                try:
                    query = self.path.split('?')[1]
                    query_components = dict(qc.split("=") for qc in query.split("&"))
                    method = query_components.get("method", None)
                    book = query_components.get("book", None)
                except ValueError:
                    method = None
                    book = None
                logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Method = {method}, Book = {book}")
                # Parsing which map reduce to use
                if method == "word-count":
                    # Parsing which data set to use
                    if book != None:
                        # If that data set exists, then begin the word count on it
                        bookPath = Path(SOURCE_DIR+"/books/"+book)
                        if bookPath.is_file():
                            logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- {book} was found")
                            specifier = query_components.get("specifier", None)
                            data = self.wordCounter(bookPath, specifier) 
                            response_data = {'result':data}
                        # Book does not exist
                        else:
                            logging.error(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- {book} was not found")
                            response_data = {'error':f'{book} was not found'}
                    # Book was not given
                    else:
                        logging.error(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- None book provided")
                        response_data = {'error':f'No book provided'}
                # Incorrect/Invalid Command
                else:
                    logging.exception(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- {method} IS AN INVALID COMMAND")
                    response_data = {'error':f'{method} is an invalid command'}
            # Incorrectly formed URL
            except IndexError:
                logging.exception(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- {self.path} IS INCORRECTLY FORMATED")
                response_data = {'error':'invalid command formatting'}
            logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Sending Response")
            self._send_response(200, response_data)
    
    # Handles comms with the Master node for the word counter request
    def wordCounter(self, bookPath, specifier):
        request = "word count"
        logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Sending request {request}...")
        socket.send_json({"method":"word count", "book": str(bookPath), "specifier":specifier})
        #  Get the reply
        message = socket.recv_json()
        logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Received reply")
        return message

if __name__ == "__main__":  
    SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Loading from config file
    with open("config.json") as json_data_file:
        DATA = json.load(json_data_file)
    
    # Creating Logging
    logging.basicConfig(filename=SOURCE_DIR+'/logging/Server.log', level=logging.DEBUG)
    
    # Starting master node    
    master = Popen(['python ', SOURCE_DIR + "/master.py", DATA["server"]["master_zmq_port"]])
    
    
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
    logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- Server stopped.")
    master.terminate()
    logging.debug(f" {time.strftime('%H:%M:%S', time.localtime())} [ SERVER ] ---- ZMQ stopped.")