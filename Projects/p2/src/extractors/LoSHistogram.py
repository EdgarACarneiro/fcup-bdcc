from AbstractExtractor import AbstractExtractor
import apache_beam as beam

from dateutil.parser import parse
import time

from matplotlib import pyplot as plt


class Process(beam.DoFn):

    MS_TO_MIN = 1.0 / 3600.0

    def process(self, haid_entry):
        dates_array = map(lambda date: time.mktime(parse(date).timetuple()),
                          haid_entry[1])

        return [(max(dates_array) - min(dates_array)) * self.MS_TO_MIN]


class LoSHistogram(AbstractExtractor):

    def __init__(self, name):
        super(LoSHistogram, self).__init__(name)

        self.process = Process()

    def processor(self, p_collection):
        return (
            p_collection |
            beam.FlatMap(lambda el: [(el[0], el[3])]) |
            beam.GroupByKey() |
            beam.ParDo(self.process)
        )

    def output_data(self, haids, output_folder):
        self.resetPlotting()

        plt.hist(haids, alpha=0.8, bins='rice', label='LoS (in mins)', rwidth=0.8)
        plt.legend(loc='upper right')
        plt.xticks(rotation=90, fontsize=5)

        plt.title('Length of Stay Histogram', loc='left',
                  fontsize=12, fontweight=0, color='black')

        plt.savefig('%s/%s.png' % (output_folder, self.name))

    def plot(self, p_collection, output_folder):
        p_collection | \
            '%s: Output data as a plot' % self.name >> beam.ParDo(
                lambda data: self.output_data(data, output_folder)
            )
