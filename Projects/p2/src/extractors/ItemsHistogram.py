from AbstractExtractor import AbstractExtractor
import apache_beam as beam

from matplotlib import pyplot as plt


class ItemsHistogram(AbstractExtractor):

    def __init__(self, name):
        super(ItemsHistogram, self).__init__(name)

        self.process = \
            lambda elem: [elem[2]]

    def output_data(self, items, output_folder):
        self.resetPlotting()

        plt.hist(items, alpha=0.8, bins='rice', label='Items', rwidth=0.8)
        plt.xticks(rotation=90, fontsize=5)

        plt.title('Items Histogram', loc='left',
                  fontsize=12, fontweight=0, color='black')
        plt.xlabel('Item ID')
        plt.ylabel('Administration Frequency')

        self.legend_and_save(output_folder)
