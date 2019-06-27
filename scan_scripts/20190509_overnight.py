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


###### measure Cylinder 1
maw("tudsmi_m1", 42.97)

if FILEWRITER:
        fw_startandwait()
sleep(36000)
if FILEWRITER:
        fw_stopandwait()


###### measure  Cylinder 2
maw("tudsmi_m1", 197.47)

if FILEWRITER:
        fw_startandwait()
sleep(36000)
if FILEWRITER:
        fw_stopandwait()
