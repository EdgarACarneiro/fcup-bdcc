from AbstractExtractor import AbstractExtractor
import apache_beam as beam

from dateutil.parser import parse
import time

from matplotlib import pyplot as plt
import pandas as pd


class Process(beam.DoFn):
    """DoFn to filter patient's data"""

    def process(self, elem):
        return [[time.mktime(parse(elem[3]).timetuple()),
                 float(elem[7])]]


class ValuesPerTime(AbstractExtractor):

    def __init__(self, name):
        super(ValuesPerTime, self).__init__(name)

        self.process = Process()

    def plot(self, p_collection, output_folder):
        # Creating Pandas Dataframe
        df = pd.DataFrame(columns=['datetime', 'value'])

        p_collection | \
            'Gathering Data on List' >> beam.combiners.ToList() | \
            'Output data as plot' >> beam.ParDo(
                lambda data: output_data(data, df, output_folder, self.name)
            )


def output_data(data_list, dataframe, output_folder, name):
    for entry in data_list:
        dataframe = dataframe.append(
            {'datetime': entry[0],
             'value': entry[1]},
            ignore_index=True
        )

    plt.style.use('seaborn')
    plt.plot('datetime', 'value', data=dataframe,
             marker='o', color='mediumvioletred')

    plt.title('Values per Date', loc='left',
              fontsize=12, fontweight=0, color='black')
    plt.xlabel('Datetime (in ms since 01/01/1970)')
    plt.ylabel('Values')

    plt.savefig('%s/%s.png' % (output_folder, name))
