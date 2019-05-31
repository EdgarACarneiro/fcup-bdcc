import apache_beam as beam
from dateutil.parser import parse

from AbstractExtractor import AbstractExtractor


class ValuesPerTime(AbstractExtractor):

    def __init__(self, name):
        super(ValuesPerTime, self).__init__(name)

        self.process = Process()

    def plot(self, p_collection, output_folder):
        print(p_collection)


class Process(beam.DoFn):
    """DoFn to filter patient's data"""

    def process(self, elem):
        return [parse(elem[3]), float(elem[7])]
