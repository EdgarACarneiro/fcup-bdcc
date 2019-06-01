from AbstractExtractor import AbstractExtractor
import apache_beam as beam
from dateutil.parser import parse
import time


class ValuesPerTime(AbstractExtractor):

    def __init__(self, name):
        super(ValuesPerTime, self).__init__(name)

        self.process = Process()

    def plot(self, p_collection, output_folder):
        print(p_collection)


class Process(beam.DoFn):
    """DoFn to filter patient's data"""

    def process(self, elem):
        return [time.mktime(parse(elem[3]).timetuple()),
                float(elem[7])]
