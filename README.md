# V20 NICOS Scanning
NICOS and just-bin-it script for scanning at V20.

## Requirements
* kafka-python
* flatbuffers
* numpy

# Scanning for V20

## Start just-bin-it

```
>> cd just-bin-it
>> python main.py -b localhost -t hist_commands
```
Replace localhost with the correct Kafka broker address. See just-bin-it's README for more information.

## Edit scan.py

Adjust all the TODOs in the file.

The following variables define the scanning parameters:

* POSITIONS_1 = positions that motor 1 must go through
* POSITIONS_2 = positions that motor 2 must go through
* NICOS_MOTOR_1 = the NICOS name for motor 1
* NICOS_MOTOR_2 = the NICOS name for motor 2
* KAFKA_ADDRESS = the Kafka broker address
* JUST_BIN_IT_COMMAND_TOPIC = the topic to send commands to just-bin-it on
* EVENT_TOPIC = the topic containing the neutron events
* HISTOGRAM_TOPIC = the topic which just-bin-it will write its results
* COUNT_TIME_SECS = how long to collect data for at each point

## Run the scan
From NICOS, use the editor to load the script.
Run it.

After some time the results will be printed to screen, for example:

```
[2566.0, 1227.0, 5969.0, 1232.0, 3007.0, 8894.0]
[8745.0, 6204.0, 4401.0, 3557.0, 3588.0, 7555.0]
[5756.0, 4421.0, 1432.0, 6188.0, 8027.0, 1429.0]
[3397.0, 6081.0, 1647.0, 1626.0, 3068.0, 8130.0]
[8117.0, 1827.0, 5532.0, 2186.0, 4141.0, 1066.0]
[2980.0, 2310.0, 3621.0, 6988.0, 9392.0, 3590.0]
```

Horizontal (left to right) is motor 2 positions
Vertical (top to bottom) is motor 1 positions

