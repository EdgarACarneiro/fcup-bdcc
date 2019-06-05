from abc import ABCMeta, abstractmethod
import apache_beam as beam


class AbstractExtractor:

    __metaclass__ = ABCMeta

    def __init__(self, name):
        """Specific class settings, such as the processing,
        should be defined here"""
        self.name = name
        self.process = None  # To be overriden

    def __process(self, p_collection):
        """Filter columns of interest and make operations over them.
        Initial dataset Schema:
        "ROW_ID","SUBJECT_ID","HADM_ID","ICUSTAY_ID","ITEMID","CHARTTIME","STORETIME",
        "CGID","VALUE","VALUENUM","VALUEUOM","WARNING","ERROR","RESULTSTATUS","STOPPED"""
        return (
            p_collection |
            '%s: Get columns of interest' % self.name >> beam.ParDo(self.process)
        )

    def columnToList(self, p_collection, col):
        """Transforms the given column in a list, if it fits in memory"""
        return(
            p_collection |
            'Get columnn %d' % col >> beam.FlatMap(
                lambda row: [row[col]]
            )
        )

    @abstractmethod
    def plot(self, p_collection, output_folder):
        """Specific class Method to transform the previously
        filtered data in human knowledge"""

    def extract(self, p_collection, output_folder):
        """Converts the given PCollection into human understandable data
        in a visual form (plots), using the previously defined process function"""
        self.plot(
            self.__process(p_collection),
            output_folder
        )
