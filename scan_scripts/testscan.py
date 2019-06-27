import sys
# TODO: Set this path to where the fbschemas folder is
sys.path.append("/hzb/caress/mike/v20_nicos_scanning")

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import time
import json
import fbschemas.hs00.EventHistogram as EventHistogram
import fbschemas.hs00.ArrayDouble as ArrayDouble
from fbschemas.hs00.Array import Array
import numpy as np
from math import sqrt


# TODO: Edit these settings as appropriate
POSITIONS_1 = np.arange(-12.8, -11.1, 0.1)
#POSITIONS_2 = [0, 1, 2, 3, 4, 5]
NICOS_MOTOR_1 = "slit2"
#NICOS_MOTOR_2 = "mcu01_m2"
KAFKA_ADDRESS = ["192.168.1.80:9092"]
JUST_BIN_IT_COMMAND_TOPIC = "hist_commands"
EVENT_TOPIC = "denex_detector"
HISTOGRAM_TOPIC = "hist_topic"
COUNT_TIME_SECS = 10

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
    last_value = 0

    # TODO: start filewriter
    fw_startandwait()

    for i in POSITIONS_1:
        # Move motor 1 to position
        print("Moving 1 to {}...".format(i))
        #maw(NICOS_MOTOR_1, i)
        maw(NICOS_MOTOR_1, (i, 0, 0.1, 40))

        last_value = get_total_counts(consumer, topic)
        print("Value after move = {}".format(last_value))

        # Read actual positions
        centerx, centery, width, height = slit2.read()

        # Collect data for some number of seconds
        print("Counting for {} seconds...".format(COUNT_TIME_SECS))
        time.sleep(COUNT_TIME_SECS)

        # Get total counts
        next_value = get_total_counts(consumer, topic)

        # Counts for "data collection" is current count minus the counts after move
        outputs.append((centerx, centery, width, height, next_value - last_value))
        last_value = next_value
        print("Value after counting = {}".format(last_value))

    # Print results
    print(outputs)

    file = open("/hzb/caress/mike/%s_scan.dat" % (NICOS_MOTOR_1,), "w")
    file.write("\t".join(["CenterX", "CenterY", "Width", "Height", "Counts", "Error"]) + "\n")
    for cx, cy, w, h, count in outputs:
        file.write("%.3f \t%.3f \t%.3f \t%.3f \t%d \t%.3f\n" % (cx, cy, w, h, count, sqrt(count)))

    file.close()

    # TODO: stop filewriter
    fw_stopandwait()

    #row = 0
    #num_rows = len(histogram) / len(POSITIONS_1)
    #while row < num_rows:
    #    start = row * len(POSITIONS_1)
    #    print(histogram[start : start + len(POSITIONS_2)])
    #    row += 1

do_scan()
