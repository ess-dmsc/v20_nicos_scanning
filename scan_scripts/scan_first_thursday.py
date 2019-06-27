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
pos_lin1_I0 = 250

COUNT_TIME_SECS_1 = 28800


COUNT_TIME_SECS_1_I0 = 7200


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

###### measure angle 1 I0
maw("lin1", pos_lin1_I0)

if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_1_I0)
if FILEWRITER:
        fw_stopandwait()
