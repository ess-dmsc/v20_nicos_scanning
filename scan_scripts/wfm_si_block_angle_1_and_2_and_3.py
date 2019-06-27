import sys
# Set this path to where the fbschemas folder is
sys.path.append("/hzb/caress/mike/v20_nicos_scanning")

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import time
import json
import fbschemas.hs00.EventHistogram as EventHistogram
import fbschemas.hs00.ArrayDouble as ArrayDouble
from fbschemas.hs00.Array import Array
import numpy as np
from math import sqrt
from datetime import datetime

### set values from alignment
angle_offset = -0.35

angle1 = 0.3 + angle_offset
angle2 = 0.9 + angle_offset
angle3 = 2.0 + angle_offset

pos_lin1_in = 168.55
pos_lin1_I0 = 250
pos_chi = 0.0 

COUNT_TIME_SECS_1 = 27000
COUNT_TIME_SECS_2 = 19800 
COUNT_TIME_SECS_3 = 15300

COUNT_TIME_SECS_2_I0 = 3600 
COUNT_TIME_SECS_3_I0 = 1800

FILEWRITER = True  # Write Kafka based Filewriter: True or False
###

def fw_isbusy():
    return NexusFileWriter.curstatus[0] != 200

def fw_startandwait():
    start_filewriter()
    while not fw_isbusy():
        sleep(0.1)
    NexusFileWriter.curstatus[1]

def fw_stopandwait():
    stop_filewriter()
    while fw_isbusy():
        sleep(2)

###

###### measure angle 1
maw("lin1", pos_lin1_in)
maw("omega", angle1)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_1)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 2
maw("omega", angle2)
maw("slit2",(-11.7, -2.0, 2.0, 25.0))
maw("slit2",(-11.7, -2.0, 0.5, 25.0))
maw("slit3h_gap", 2)
maw("slit3h_gap", 0.5)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_2)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 2 I0
maw("lin1", pos_lin1_I0)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_2_I0)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 3 I0
maw("slit2",(-11.7, -2.0, 2.0, 25.0))
maw("slit2",(-11.7, -2.0, 0.7, 25.0))
maw("slit3h_gap", 2)
maw("slit3h_gap", 0.7)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_3_I0)
if FILEWRITER:
        fw_stopandwait()


###### measure angle 3
maw("lin1", pos_lin1_in)
maw("omega", angle3)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_3)
if FILEWRITER:
        fw_stopandwait()