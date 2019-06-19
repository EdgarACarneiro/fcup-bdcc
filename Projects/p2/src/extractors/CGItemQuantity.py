from AbstractExtractor import AbstractExtractor
import apache_beam as beam
import pandas as pd
from pandas.plotting import parallel_coordinates


from matplotlib import pyplot as plt
from matplotlib.colors import ListedColormap
import seaborn as sns


class CGItemQuantity(AbstractExtractor):

    def __init__(self, name):
        super(CGItemQuantity, self).__init__(name)

        self.process = \
            lambda el: [(
                int(el[5]),
                el[2],
                float(el[7])
            )]

    @staticmethod
    def __round_to_base(value, base=5):
        return int(base * round(float(value)/base))

    def output_data(self, data, output_folder):
        self.resetPlotting()

        # Getting max and min to find round base
        max_v = max(data, key=lambda el: el[2])[2]
        min_v = min(data, key=lambda el: el[2])[2]
        round_base = round((max(data, key=lambda el: el[2])[2] -
                            min(data, key=lambda el: el[2])[2]) / 6)

        df = pd.DataFrame(
            columns=["Care Giver", "Item", "Quantity"],
            data=map(lambda el:
                     (el[0],
                      el[1],
                      self.__round_to_base(el[2], round_base)),
                     data)
        )

        sns.cubehelix_palette(dark=.3, light=.8, as_cmap=True)
        sns.scatterplot(x="Item", y="Care Giver",
                        hue="Quantity", size="Quantity",
                        sizes=(50, 300), hue_norm=(min_v, max_v),
                        legend="full", data=df)

        plt.legend(loc='upper right')
        plt.title('CGI - Item - Quantity Relation', loc='left',
                  fontsize=12, fontweight=0, color='black')
        plt.xticks(rotation='vertical')

        plt.savefig('%s/%s.png' % (output_folder, self.name))

    def plot(self, p_collection, output_folder):
        p_collection | \
            '%s: Output data as a plot' % self.name >> beam.ParDo(
                lambda data: self.output_data(data, output_folder)
            )
