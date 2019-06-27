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
POSITIONS = np.arange(-3, 3, 0.1)
#POSITIONS = [(-11.7, x, 15, 0.2) for x in np.arange(-5, 5, 0.1))
COLUMNS = [  # Extra columns of interest; Counts and Error always added later
    #("CenterX", slit3h_center, "%.3f"),
    #("CenterY", slit3v_center, "%.3f"),
    #("Width", slit3h_gap, "%.3f"),
    #("Height", slit3v_gap, "%.3f"),
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

def fw_stopandwait():
    stop_filewriter()
    while fw_isbusy():
        sleep(2)



def do_scan():
    outputs = [[10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9380.0, 96.85040010242601], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8975.0, 94.73647660748209], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9185.0, 95.83840566286565], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9063.0, 95.19978991573458], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9192.0, 95.87491851365508], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9181.0, 95.81753492967766], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9103.0, 95.40964311850244], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9136.0, 95.58242516278817], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9046.0, 95.11046209539727], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9079.0, 95.28378665859161], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9156.0, 95.68698971124549], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9193.0, 95.88013350011565], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9142.0, 95.61380653441218], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9067.0, 95.22079604792222], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9144.0, 95.62426470305536], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9172.0, 95.77055915050303], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8670.0, 93.11283477587824], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8596.0, 92.71461589199407], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 7617.0, 87.27542609463445], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 7697.0, 87.73254812211943], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8175.0, 90.41570660012562], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8639.0, 92.94622100978609], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8680.0, 93.16651759081692], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8620.0, 92.84395510748128], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8680.0, 93.16651759081692], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8649.0, 93.0], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8603.0, 92.7523584605804], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8652.0, 93.01612763386788], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8648.0, 92.99462350050136], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8603.0, 92.7523584605804], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 9000.0, 94.86832980505137], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8602.0, 92.74696760541555], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8642.0, 92.9623579735368], [10.620361328125, 2.0001220703125, 3.0, 30.000244140625, 8678.0, 93.15578350268973]]
    columns = []
    last_value = 0

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
        print "".join(value.ljust(width) for value in row)

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    file = open(SCAN_LOCATION + "%s_%s.dat" % (SCAN_NAME, timestamp), "w")

    file.write("\t".join(headers) + "\n")
    format = "\t".join(format) + "\n"
    for row in outputs:
        file.write(format % tuple(row))

    file.close()


do_scan()
