from AbstractExtractor import AbstractExtractor
import apache_beam as beam
import pandas as pd

from matplotlib import pyplot as plt
from matplotlib.colors import ListedColormap
import seaborn as sns


class Process(beam.DoFn):

    def process(self, item):
        hours = [0] * 24

        for hour in item[1]:
            hours[int(
                hour.split(' ')[1].split(":")[0])] += 1

        return [[item[0], hours]]


class StackedDailyItems(AbstractExtractor):

    NUM_INDIVIDUAL_ITEMS = 10

    def __init__(self, name):
        super(StackedDailyItems, self).__init__(name)

        self.process = Process()

    def processor(self, p_collection):
        return self.collection_to_list(
            p_collection |
            '%s: Get columns of interest' % self.name >> beam.FlatMap(
                lambda el: [(el[2], el[3])]) |
            '%s: Grouping by Item' % self.name >> beam.GroupByKey() |
            '%s: Processing Item\'s data' % self.name >> beam.ParDo(
                self.process)
        )

    def output_data(self, items, output_folder):
        self.resetPlotting()

        # Listing not so relevant items
        others = []
        final_items = None

        # Dinamically aggregatting data in others
        counter = 0
        while(final_items == None or len(final_items) > self.NUM_INDIVIDUAL_ITEMS):
            # Conidition was not met, restart algorithm with higher counter
            final_items = []
            others = []
            counter += 1

            # Populate others array
            for item in items:
                if sum(item[1]) <= counter:
                    others.append(item)
                else:
                    final_items.append(item)

        # Aggregating those in 'other' item
        others_agg = [0] * 24
        for o in others:
            for i in range(len(o[1])):
                others_agg[i] += o[1][i]

        # Joining others to items
        final_items += [['Others', others_agg]]

        # Final items
        df = pd.DataFrame(
            columns=["Items"] + [str(i) for i in range(0, 24)],
            data=[[item[0]] + item[1] for item in final_items]
        )

        sns.set()
        df.set_index('Items').T.plot(kind='bar', stacked=True,
                                     colormap=ListedColormap(sns.color_palette("GnBu", 10)))
        plt.xlabel('Hour')

        plt.title('Items Hours Intake', loc='left',
                  fontsize=12, fontweight=0, color='black')

        self.legend_and_save(output_folder)
