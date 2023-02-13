#!/bin/bash

python3 client1.py &
python3 client2.py &
python3 failclient.py &

wait
