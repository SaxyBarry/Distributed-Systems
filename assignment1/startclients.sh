#!/bin/bash

python client1.py &
python client2.py &
python failclient.py &

wait