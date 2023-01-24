import threading
import time

def task():
    time.sleep(3)
    print("B")

threads = [threading.Thread(target=task), threading.Thread(target=task)]
for x in threads:
    x.start()
    print("A")
print("C")
