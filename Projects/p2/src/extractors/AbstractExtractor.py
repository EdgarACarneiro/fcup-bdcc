from abc import ABC, abstractmethod
import apache_beam as beam


class AbstractExtractor(ABC):

    process: beam.DoFn
    name: str

    def __init__(self, name):
        """Specific class settings, such as the processing,
        should be defined here"""
        self.name = name
        pass

    def __process(self, p_collection):
        """Filter columns of interest and make operations over them.
        Initial dataset Schema:
        "ROW_ID","SUBJECT_ID","HADM_ID","ICUSTAY_ID","ITEMID","CHARTTIME","STORETIME",
        "CGID","VALUE","VALUENUM","VALUEUOM","WARNING","ERROR","RESULTSTATUS","STOPPED"""
        return (
            p_collection |
            'Get columns of interest' >> beam.ParDo(self.process())
        )

    @abstractmethod
    def __plot(self, p_collection, output_folder):
        """Specific class Method to transform the previously
        filtered data in human knowledge"""
        pass

    def extract(self, p_collection, output_folder):
        """Converts the given PCollection into human understandable data
        in a visual form (plots), using the previously defined process function"""
        self.__plot(
            self.__process(p_collection),
            output_folder
        )
