#!/usr/bin/python

# 2013 Benjamin Lau, Mashery
# Copyright: CC0
# Minor Changes by Bob

import sys
import fileinput
import json
import datetime
import time
import socket
from threading import Timer


CARBON_SERVER="127.0.0.1"
CARBON_PORT=2003
timeout = 10
socket.setdefaulttimeout(timeout)

counts = {}
counter = 0
duration = 1 #adjust this to make the polling period shorter for testing

def process_input(line):
    global counter
    global counts
    json_dump = json.loads(line)
    if counts.has_key(json_dump['name']):
        counts[json_dump['name']]+=1
    else:
        counts[json_dump['name']] = 1
#    print counts
    counter+=1
    
def send_to_graphite():    
    global counts
    global mytimer
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    stats = []
    for key in counts.keys():
        stats.append("openxc.vehicle.data.%s %d %d" % (key,counts[key],time.mktime(time.strptime(counts[ts],'%Y-%m-%d %H:%M'))))
    msg='\n'.join(stats)
    if msg == "":
        print "No data for this period"
    else:
        print msg
        #comment out this block if you need to test and don't have a local instance of carbon/graphite
        try:
          sock=socket.socket()
          sock.connect((CARBON_SERVER,CARBON_PORT))
          sock.sendall(msg)
        except:
          print "Error connecting to %s:%d"%(CARBON_SERVER,CARBON_PORT)
    counts = {}
    mytimer = Timer(duration, send_to_graphite)
    mytimer.start()
    process()

mytimer = Timer(duration, send_to_graphite)
    
def process():
    global mytimer
    while 1:
        line = sys.stdin.readline()
        if not line: break
        sys.stdout.write("processing: %s" % line)
        process_input(line)

def hook_nobuf(filename, mode):
    return open(filename, mode, 0)
                    
fi = fileinput.FileInput(openhook=hook_nobuf)
mytimer.start()
process()

