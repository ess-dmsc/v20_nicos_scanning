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

# TODO: Edit these scan settings as appropriate
SCAN_LOCATION = "/hzb/caress/mike/v20_nicos_scanning/scans/"
SCAN_NAME = "omega"
NICOS_MOTOR = "omega"
POSITIONS = np.arange(-1.35, 0.66, 0.1)
COLUMNS = [  # Extra columns of interest; Counts and Error always added later
    ("Position", omega, "%.3f"),
]
COUNT_TIME_SECS = 10
FILEWRITER = True  # Write Kafka based Filewriter: True or False

# TODO: Kafka settings
KAFKA_ADDRESS = ["192.168.1.80:9092"]
JUST_BIN_IT_COMMAND_TOPIC = "hist_commands"
EVENT_TOPIC = "denex_detector"
HISTOGRAM_TOPIC = "hist_topic"

CONFIG = {
    "data_brokers": KAFKA_ADDRESS,
    "data_topics": [EVENT_TOPIC],
    "histograms": [
        {
            "type": "sehist1d",
            "tof_range": [0, 100000000],
            "num_bins": 50,
            "topic": HISTOGRAM_TOPIC,
        }
    ],
}


def deserialise_hs00(buf):
    """
    Convert flatbuffer into a histogram.

    :param buf:
    :return: dict of histogram information
    """
    event_hist = EventHistogram.EventHistogram.GetRootAsEventHistogram(buf, 0)

    dims = []
    for i in range(event_hist.DimMetadataLength()):
        bins_fb = event_hist.DimMetadata(i).BinBoundaries()

        # Get bins
        temp = ArrayDouble.ArrayDouble()
        temp.Init(bins_fb.Bytes, bins_fb.Pos)
        bins = temp.ValueAsNumpy()

        # Get type
        if event_hist.DimMetadata(i).BinBoundariesType() == Array.ArrayDouble:
            bin_type = np.float64
        else:
            raise TypeError("Type of the bin boundaries is incorrect")

        info = {
            "length": event_hist.DimMetadata(i).Length(),
            "edges": bins.tolist(),
            "type": bin_type,
        }
        dims.append(info)

    # Get the data
    if event_hist.DataType() != Array.ArrayDouble:
        raise TypeError("Type of the data array is incorrect")

    data_fb = event_hist.Data()
    temp = ArrayDouble.ArrayDouble()
    temp.Init(data_fb.Bytes, data_fb.Pos)
    data = temp.ValueAsNumpy()
    shape = event_hist.CurrentShapeAsNumpy().tolist()

    hist = {
        "source": event_hist.Source().decode("utf-8"),
        "shape": shape,
        "dims": dims,
        "data": data.reshape(shape),
    }
    return hist


def get_total_counts(consumer, topic):
    data = {}
    consumer.seek_to_end(topic)

    while len(data) == 0:
        data = consumer.poll(5)
    ans = deserialise_hs00(data[topic][-1].value)
    return sum(ans["data"])


def fw_isbusy():
    return NexusFileWriter.curstatus[0] != 200

def fw_startandwait():
    start_filewriter()
    while not fw_isbusy():
        sleep(0.1)
    print(NexusFileWriter.curstatus[1])

def fw_stopandwait():
    stop_filewriter()
    while fw_isbusy():
        sleep(2)



def do_scan():
    # Configure Kafka and just-bin-it
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDRESS)
    producer.send(JUST_BIN_IT_COMMAND_TOPIC, bytes(json.dumps(CONFIG)).encode("utf-8"))
    producer.flush()
    time.sleep(2)

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_ADDRESS)
    topic = TopicPartition(HISTOGRAM_TOPIC, 0)
    consumer.assign([topic])
    consumer.seek_to_end(topic)

    outputs = []
    columns = []
    last_value = 0

    # Start filewriter
    if FILEWRITER:
        fw_startandwait()

    for pos in POSITIONS:
        # Move motor 1 to position
        #print("Moving to {}...".format(pos))
        maw(NICOS_MOTOR, pos)

        last_value = get_total_counts(consumer, topic)
        print("Counts after move = {}".format(last_value))

        # Read actual positions
        columns = []
        for n, device, f in COLUMNS:
            columns.append(device.read())

        # Collect data for some number of seconds
        print("Counting for {} seconds...".format(COUNT_TIME_SECS))
        time.sleep(COUNT_TIME_SECS)

        # Get total counts
        next_value = get_total_counts(consumer, topic)

        # Counts for "data collection" is current count minus the counts after move
        counts = next_value - last_value
        error = sqrt(counts)
        columns.append(counts)
        columns.append(error)
        outputs.append(columns)
        #print(columns)

        last_value = next_value
        print("Counts after waiting = {}".format(last_value))

    # Print results
    print(outputs)

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    file = open(SCAN_LOCATION + "%s_%s.dat" % (SCAN_NAME, timestamp), "w")

    headers = [name for name, d, f in COLUMNS]
    headers.append("Counts")
    headers.append("Error")
    file.write("\t".join(headers) + "\n")

    format = [f for n, d, f in COLUMNS]
    format.append("%d")  # Counts
    format.append("%.3f")  # Error
    format = "\t".join(format) + "\n"
    for row in outputs:
        file.write(format % tuple(row))

    file.close()

    # Stop filewriter
    if FILEWRITER:
        fw_stopandwait()

do_scan()
