import apache_beam as beam
from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
from datetime import date, datetime

from AbstractExtractor import AbstractExtractor


class ValuesPerTime(AbstractExtractor):

    def __init__(self, name):
        super(ValuesPerTime, self).__init__(name)

        self.process = \
            lambda elem: [[elem[3],
                           float(elem[7]),
                           elem[9],
                           elem[10]]]

    @staticmethod
    def __timestamp(dt):
        return (dt - datetime(1970, 1, 1)).total_seconds()

    def output_data(self, data_list, output_folder):
        self.resetPlotting()

        df = pd.DataFrame(columns=['datetime', 'value', 'measurement'])

        # Populating Dataframe
        for entry in data_list:
            df = df.append(
                {
                    'datetime': self.__timestamp(datetime.strptime(entry[0], '%Y-%m-%d %H:%M:%S')),
                    'value': entry[1],
                    'measurement': 'Error' if entry[3] == '1' else
                    ('Warning' if entry[2] == '1' else 'Normal')
                },
                ignore_index=True
            )
        df = df.sort_values('datetime', ascending=True)

        # Plotting
        ax = sns.scatterplot(x='datetime', y='value',
                             hue='measurement', style='measurement', data=df,
                             palette=sns.color_palette(['#5392e0', '#f7bf27', '#c62901']))

        # Renaming the x-Axis
        new_labels = [datetime.fromtimestamp(item).strftime(
            '%Y-%m-%d') for item in ax.get_xticks()]
        ax.set_xticklabels(new_labels)

        plt.title('Administered values per Date', loc='left',
                  fontsize=12, fontweight=0, color='black')

        plt.ylabel('Values')
        plt.xlabel('ChartTime')
        plt.xticks(rotation='vertical')
        plt.tight_layout()

        self.legend_and_save(output_folder)

    def plot(self, p_collection, output_folder):
        p_collection | \
            '%s: Output data as a plot' % self.name >> beam.ParDo(
                lambda data: self.output_data(data, output_folder)
            )
