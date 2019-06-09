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
                int(el[2]),
                float(el[7])
            )]

    def output_data(self, data, output_folder):
        self.resetPlotting()

        df = pd.DataFrame(
            columns=["Care Giver", "Item", "Quantity"],
            data=data
        )

        parallel_coordinates(df, "Item", colormap=ListedColormap(
            sns.color_palette("GnBu", 10)))

        plt.legend(loc='upper right')
        plt.title('CGI - Item - Quantity Relation', loc='left',
                  fontsize=12, fontweight=0, color='black')

        plt.savefig('%s/%s.png' % (output_folder, self.name))

    def plot(self, p_collection, output_folder):
        p_collection | \
            '%s: Output data as a plot' % self.name >> beam.ParDo(
                lambda data: self.output_data(data, output_folder)
            )
