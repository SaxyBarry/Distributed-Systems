import threading
import time

def task():
    time.sleep(3)
    print("B")

threads = []
x = 15
while True:
    y = threading.Thread(target=task)
    y.start()
    print("A")
    x -= 1
    if x < 0:
        break
print("C")
