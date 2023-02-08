from http.server import BaseHTTPRequestHandler, HTTPServer
from subprocess import Popen
import os
import time
import zmq

class Server(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(bytes("<html><head><title>Assignment 2</title></head>", "utf-8"))
        self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
        self.wfile.write(bytes("<body>", "utf-8"))
        if self.path == "/word-count":
            self.wordCounter()
        else:
            self.wfile.write(bytes("<p>nothing</p>", "utf-8"))
        self.wfile.write(bytes("</body></html>", "utf-8"))
        
    def wordCounter(self):
        self.wfile.write(bytes("<p>word counter</p>", "utf-8"))
        request = "word count"
        print(f"Sending request {request}...")
        socket.send_string(request)
        #  Get the reply
        message = socket.recv()
        print("Received reply %s [ %s ]" % (request, message))


if __name__ == "__main__":    
    # Starting master node    
    SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
    master = Popen(['python.exe', SOURCE_DIR + "\master.py"])
    
    #  Starting ZeroMQ
    print("Connecting to ZMQ server...")
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")

    # Starting HTTP Server for API connection
    hostName = "localhost"
    serverPort = 8080
    webServer = HTTPServer((hostName, serverPort), Server)
    print(f"Server started http://{hostName}:{serverPort}")
    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass
    # Stopping Server and ZMQ
    webServer.server_close()
    print("Server stopped.")
    master.terminate()
    print("ZMQ stopped.")