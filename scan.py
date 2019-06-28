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
from datetime import datetime
import random


# TODO: Edit these scan settings as appropriate
SCAN_LOCATION = "/hzb/caress/mike/v20_nicos_scanning/scans/"
SCAN_NAME = "slit3_vertical"
NICOS_MOTOR = "slit3v_center"
POSITIONS = np.arange(-5, 5, 0.1)
COLUMNS = [  # Extra columns of interest; Counts and Error always added later
    ("CenterX", slit3h_center, "%.3f"),
    ("CenterY", slit3v_center, "%.3f"),
    ("Width", slit3h_gap, "%.3f"),
    ("Height", slit3v_gap, "%.3f"),
]
COUNT_TIME_SECS = 10
FILEWRITER = False

# TODO: Kafka settings
KAFKA_ADDRESS = ["192.168.1.80:9092"]
JUST_BIN_IT_COMMAND_TOPIC = "hist_commands"
EVENT_TOPIC = "denex_detector"
HISTOGRAM_TOPIC = "hist_topic"


def generate_config(id, count_time):
    return {
        "cmd": "config",
        "data_brokers": KAFKA_ADDRESS,
        "data_topics": [EVENT_TOPIC],
        "interval": count_time,
        "histograms": [
            {
                "type": "sehist1d",
                "tof_range": [0, 100_000_000],
                "num_bins": 50,
                "topic": HISTOGRAM_TOPIC,
                "id": id
            }
        ],
    }


def deserialise_hs00(buf):
    """
    Convert flatbuffer into a histogram.
    :param buf:
    :return: dict of histogram information
    """
    # Check schema is correct
    # if get_schema(buf) != "hs00":
    #     raise Exception(
    #         f"Incorrect schema: expected hs00 but got {get_schema(buf)}")

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

        hist_info = {
            "length": event_hist.DimMetadata(i).Length(),
            "edges": bins.tolist(),
            "type": bin_type,
        }
        dims.append(hist_info)

    # Get the data
    if event_hist.DataType() != Array.ArrayDouble:
        raise TypeError(
            "Type of the data array is incorrect")  # pragma: no mutate

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
        "info": event_hist.Info().decode("utf-8")
        if event_hist.Info()
        else "",  # pragma: no mutate
    }
    return hist


def send_config(config):
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDRESS)
    producer.send(JUST_BIN_IT_COMMAND_TOPIC, bytes(json.dumps(config), "utf-8"))
    producer.flush()
    time.sleep(2)


def get_total_counts(consumer, topic, id, seconds):
    config = generate_config(id, seconds)
    send_config(config)

    data = {}
    consumer.seek_to_end(topic)
    pos = consumer.position(topic)
    if pos == 0:
        raise Exception("Could not find data in topic")

    consumer.seek(topic, pos - 1)

    while True:
        while len(data) == 0:
            data = consumer.poll(5)
        ans = deserialise_hs00(data[topic][-1].value)
        data = {}
        status = json.loads(ans["info"])
        if status["id"] == id and status["state"] == "FINISHED":
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
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_ADDRESS)
    topic = TopicPartition(HISTOGRAM_TOPIC, 0)
    consumer.assign([topic])
    consumer.seek_to_end(topic)

    outputs = []
    columns = []

    # Start filewriter
    if FILEWRITER:
        fw_startandwait()

    for pos in POSITIONS:
        # Move motor to position
        print("Moving to {}...".format(pos))
        maw(NICOS_MOTOR, pos)

        # Read actual positions
        columns = []
        for n, device, f in COLUMNS:
            columns.append(device.read())

        # Collect data for some number of seconds
        # Note: may take longer in "real time" than expected due to delays in data
        # processing
        print("Counting for {} seconds of data...".format(COUNT_TIME_SECS))
        id = "id-{}".format(random.randint(1, 10000000))
        counts = get_total_counts(consumer, topic, id, COUNT_TIME_SECS)

        error = sqrt(counts)
        columns.append(counts)
        columns.append(error)
        outputs.append(columns)
        print(columns)

    # Print results
    print(outputs)

    # Pretty print
    headers = [name for name, d, f in COLUMNS]
    headers.append("Counts")
    headers.append("Error")

    format = [f for n, d, f in COLUMNS]
    format.append("%d")  # Counts
    format.append("%.3f")  # Error

    data = []
    for row in outputs:
        data.append([f % (v,) for f, v in zip(format, row)])

    width = max(len(x) for x in headers)
    width = max(width, *[len(str(value)) for value in row for row in data])
    width = width + 4

    for row in [headers] + data:
        print("".join(value.ljust(width) for value in row))

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    file = open(SCAN_LOCATION + "%s_%s.dat" % (SCAN_NAME, timestamp), "w")

    file.write("\t".join(headers) + "\n")
    format = "\t".join(format) + "\n"
    for row in outputs:
        file.write(format % tuple(row))

    file.close()

    # Stop filewriter
    if FILEWRITER:
        fw_stopandwait()


do_scan()
