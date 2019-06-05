from AbstractExtractor import AbstractExtractor
import apache_beam as beam

from matplotlib import pyplot as plt


class ItemsHistogram(AbstractExtractor):

    def __init__(self, name):
        super(ItemsHistogram, self).__init__(name)

        self.process = \
            lambda elem: [elem[2]]

    def output_data(self, items, output_folder):
        # plt.style.use('seaborn')
        plt.clf()

        plt.hist(items, alpha=0.8, label='Items', rwidth=0.8)
        plt.legend(loc='upper right')
        plt.savefig('%s/%s.png' % (output_folder, self.name))

        # plt.style.use('seaborn')
        # plt.plot('datetime', 'value', data=dataframe, marker='o',
        #          markersize=4, color='mediumvioletred', linestyle='none')

        # plt.title('Values per Date', loc='left',
        #           fontsize=12, fontweight=0, color='black')
        # plt.xlabel('Datetime (in ms since 01/01/1970)')
        # plt.ylabel('Values')

        plt.savefig('%s/%s.png' % (output_folder, self.name))

    def plot(self, p_collection, output_folder):
        # Creating Pandas Dataframe

        p_collection | \
            '%s: Gathering Data on List' % self.name >> beam.combiners.ToList() | \
            '%s: Output data as a plot' % self.name >> beam.ParDo(
                lambda data: self.output_data(data, output_folder)
            )
