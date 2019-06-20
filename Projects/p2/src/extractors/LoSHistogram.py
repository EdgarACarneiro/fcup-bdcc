from AbstractExtractor import AbstractExtractor
import apache_beam as beam
import seaborn as sns

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
        return self.collection_to_list(
            p_collection |
            '%s: Get columns of interest' % self.name >> beam.FlatMap(
                lambda el: [(el[0], el[3])]) |
            '%s: Grouping by HAID' % self.name >> beam.GroupByKey() |
            '%s: Processing HAID\'s data' % self.name >> beam.ParDo(
                self.process)
        )

    def output_data(self, haids, output_folder):
        self.resetPlotting()

        plt.title('Length of Stay', loc='left',
                  fontsize=12, fontweight=0, color='black')

        sns.distplot(haids, hist=True, bins='rice',
                     label='LoS', kde=False, rug=True)
        plt.xlabel('time (in mins)')
        plt.ylabel('Frequency')

        self.legend_and_save(output_folder)
