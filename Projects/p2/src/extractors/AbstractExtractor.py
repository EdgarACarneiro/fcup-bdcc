from abc import ABCMeta, abstractmethod
import apache_beam as beam

from matplotlib import pyplot as plt


class AbstractExtractor:

    __metaclass__ = ABCMeta

    def __init__(self, name):
        """Specific class settings, such as the processing,
        should be defined here"""
        self.name = name
        self.process = None  # To be overridden

    def collection_to_list(self, p_collection):
        """Transform a collection into a list"""
        return (
            p_collection |
            '%s: Gathering Data on List' % self.name >> beam.combiners.ToList()
        )

    def processor(self, p_collection):
        """Filter columns of interest and make operations over them.
        Initial dataset Schema:
        "ROW_ID","SUBJECT_ID","HADM_ID","ICUSTAY_ID","ITEMID","CHARTTIME","STORETIME",
        "CGID","VALUE","VALUENUM","VALUEUOM","WARNING","ERROR","RESULTSTATUS","STOPPED"""
        return self.collection_to_list(
            p_collection |
            '%s: Get columns of interest' % self.name >> beam.ParDo(
                self.process)
        )

    def column_to_list(self, p_collection, col):
        """Transforms the given column in a list, if it fits in memory"""
        return(
            p_collection |
            'Get columnn %d' % col >> beam.FlatMap(
                lambda row: [row[col]]
            )
        )

    def resetPlotting(self):
        """Reset plotting, and prepare for a new plot"""
        plt.clf()
        plt.style.use('seaborn')

    def legend_and_save(self, output_folder):
        """Insert the default legend in the plot and save the plot"""
        plt.legend(loc='upper right', frameon=True,
                   facecolor='white', framealpha=0.6)
        plt.savefig('%s/%s.png' % (output_folder, self.name))

    @abstractmethod
    def output_data(self, p_collection, output_folder):
        """Specific class Method to transform the previously
        filtered data in human knowledge"""

    def extract(self, p_collection, output_folder):
        """Converts the given PCollection into human understandable data
        in a visual form (plots), using the previously defined process function"""
        self.processor(p_collection) | \
            '%s: Output data as a plot' % self.name >> beam.ParDo(
                lambda data: self.output_data(data, output_folder)
        )
