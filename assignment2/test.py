import zmq
import time

context = zmq.Context()

# create a PULL socket
receiver = context.socket(zmq.PULL)
receiver.bind("tcp://127.0.0.1:5557")

# create a PUSH socket
sender = context.socket(zmq.PUSH)
sender.connect("tcp://127.0.0.1:5557")

# send messages using the PUSH socket
for i in range(5):
    msg = "message %s" % i
    sender.send_string(msg)
    print("Sent message: %s" % msg)
    time.sleep(1)

# receive messages using the PULL socket
while True:
    msg = receiver.recv_string()
    print("Received message: %s" % msg)