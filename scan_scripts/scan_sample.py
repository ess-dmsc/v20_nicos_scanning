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
angle2 = 0.7 + angle_offset
angle3 = 2.0 + angle_offset

pos_lin = 168.60
pos_lin1_I0 = 200
pos_chi = 0.0 
pos_beamstop_out = 270.0
pos_beamstop_in = 305

#COUNT_TIME_SECS = 10800 # for 3 hours and 4 scans = 12 hours
COUNT_TIME_SECS_1 = 300
COUNT_TIME_SECS_2 = 300
COUNT_TIME_SECS_3 = 300
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

###### measure angle 1 I_0
maw("tudsmi_m1", pos_beamstop_out)
maw("slit2",(-11.7, -2.0, 2.0, 50.0))
maw("slit2",(-11.7, -2.0, 1.0, 50.0))
maw("slit2",(-11.7, -2.0, 0.2, 50.0))
maw("slit3h_gap", 2)
maw("slit3h_gap", 0.2)

maw("lin1", pos_lin1_I0)
maw("chi", pos_chi)
maw("omega", angle_offset)
if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_1)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 2 I_0
maw("tudsmi_m1", pos_beamstop_out)
maw("slit2",(-11.7, -2.0, 2.5, 50.0))
maw("slit2",(-11.7, -2.0, 2.0, 50.0))
maw("slit2",(-11.7, -2.0, 1.5, 50.0))
maw("slit3h_gap", 2)
maw("slit3h_gap", 1.5)

maw("lin1", pos_lin1_I0)
maw("chi", pos_chi)
maw("omega", angle_offset)
if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_2)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 3 I_0
maw("tudsmi_m1", pos_beamstop_in)
maw("slit2",(-11.7, -2.0, 12.0, 50.0))
maw("slit2",(-11.7, -2.0, 11.0, 50.0))
maw("slit2",(-11.7, -2.0, 7.0, 50.0))
maw("slit3h_gap", 12.0)
maw("slit3h_gap", 7.0)

maw("lin1", pos_lin1_I0)
if FILEWRITER:
        fw_startandwait()
time.sleep(COUNT_TIME_SECS_3)
if FILEWRITER:
        fw_stopandwait()



###### measure angle 1 sample
maw("tudsmi_m1", pos_beamstop_out)
maw("slit2",(-11.7, -2.0, 2.0, 50.0))
maw("slit2",(-11.7, -2.0, 1.0, 50.0))
maw("slit2",(-11.7, -2.0, 0.2, 50.0))
maw("slit3h_gap", 2)
maw("slit3h_gap", 0.2)

maw("lin1", pos_lin)
maw("chi", pos_chi)
maw("omega", angle1)
if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_1)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 2 sample
maw("tudsmi_m1", pos_beamstop_out)
maw("slit2",(-11.7, -2.0, 2.5, 50.0))
maw("slit2",(-11.7, -2.0, 2.0, 50.0))
maw("slit2",(-11.7, -2.0, 1.5, 50.0))
maw("slit3h_gap", 2)
maw("slit3h_gap", 1.5)

maw("lin1", pos_lin)
maw("chi", pos_chi)
maw("omega", angle2)
if FILEWRITER:
        fw_startandwait()
sleep(COUNT_TIME_SECS_2)
if FILEWRITER:
        fw_stopandwait()

###### measure angle 3 sample
#maw("tudsmi_m1", pos_beamstop_in)
#maw("slit2",(-11.7, -2.0, 12.0, 50.0))
#maw("slit2",(-11.7, -2.0, 11.0, 50.0))
#maw("slit2",(-11.7, -2.0, 7.0, 50.0))
#maw("slit3h_gap", 12.0)
#maw("slit3h_gap", 7.0)

#maw("lin1", pos_lin)
#maw("chi", pos_chi)
#maw("omega", angle3)
#if FILEWRITER:
#        fw_startandwait()
#sleep(COUNT_TIME_SECS_3)
#if FILEWRITER:
#        fw_stopandwait()

####### reset values
maw("slit2",(-11.7, -2.0, 2.0, 50.0))
maw("slit2",(-11.7, -2.0, 1.0, 50.0))
maw("slit2",(-11.7, -2.0, 0.2, 50.0))
maw("slit3h_gap", 0.2)
maw("slit3h_gap", 0.2)
maw("omega", angle_offset)
maw("tudsmi_m1", pos_beamstop_out)