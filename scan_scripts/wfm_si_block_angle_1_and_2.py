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
angle_offset = 0.99

angle1 = 0.3 + angle_offset
angle2 = 0.9 + angle_offset
angle3 = 2.0 + angle_offset

pos_lin1_in = 168.60
pos_lin1_I0 = 250
pos_chi = 0.0 

COUNT_TIME_SECS_1 = 28800
COUNT_TIME_SECS_2 = 21600 

COUNT_TIME_SECS_1_I0 = 10800
COUNT_TIME_SECS_2_I0 = 3600 

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
maw("chi", pos_chi)
maw("omega", angle1)

maw("slit2",(-11.7, -2.0, 2.0, 50.0))
maw("slit2",(-11.7, -2.0, 0.2, 50.0))

maw("slit3h_gap", 2)
maw("slit3h_gap", 0.2)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_1)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 1 I0
maw("lin1", pos_lin1_I0)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_1_I0)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 2 I0
maw("lin1", pos_lin1_I0)

maw("slit2",(-11.7, -2.0, 2.0, 50.0))
maw("slit2",(-11.7, -2.0, 0.5, 50.0))

maw("slit3h_gap", 2)
maw("slit3h_gap", 0.5)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_2_I0)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 2
maw("lin1", pos_lin1_in)
maw("omega", angle2)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_2)
if FILEWRITER:
        fw_stopandwait()