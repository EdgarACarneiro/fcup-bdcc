from abc import ABCMeta, abstractmethod
import apache_beam as beam

from matplotlib import pyplot as plt


class AbstractExtractor:

    __metaclass__ = ABCMeta

    def __init__(self, name):
        """Specific class settings, such as the processing,
        should be defined here"""
        self.name = name
        self.process = None  # To be overriden

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
        plt.clf()
        plt.style.use('seaborn')

    @abstractmethod
    def plot(self, p_collection, output_folder):
        """Specific class Method to transform the previously
        filtered data in human knowledge"""

    def extract(self, p_collection, output_folder):
        """Converts the given PCollection into human understandable data
        in a visual form (plots), using the previously defined process function"""
        self.plot(
            self.processor(p_collection),
            output_folder
        )
