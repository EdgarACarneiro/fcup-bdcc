import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

from extractors.ValuesPerTime import ValuesPerTime
from extractors.ItemsHistogram import ItemsHistogram
from extractors.LoSHistogram import LoSHistogram
from extractors.StackedDailyItems import StackedDailyItems
from extractors.CGItemQuantity import CGItemQuantity

"""Your task is to perform a statistical analysis on this data
and produce timeline graphs for each patient (SUBJECT_ID)"""

# Extractors being used
extractors = [
    ValuesPerTime,
    ItemsHistogram,
    LoSHistogram,
    StackedDailyItems,
    CGItemQuantity
]


class FilterPatient(beam.DoFn):
    """DoFn to filter patient's data"""

    def __init__(self, patient_id):
        self.patient_id = patient_id

    def process(self, elem):
        # Since python2 hasn't star operator
        el_data = elem.split(",")

        # # Only return if it's this patient data
        if el_data[1] == self.patient_id:
            return [el_data[2:]]


class CollectionPrinter(beam.DoFn):
    """Helper DoFn able to print a PCollection contents"""

    def process(self, elem):
        print(elem)


def print_collection(collection):
    collection | beam.ParDo(CollectionPrinter())


def run(args):
    # Creating and opening the pipeline
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:

        patient_data = (
            p |
            'Reading Events' >> beam.io.ReadFromText(
                args.input_file, skip_header_lines=1) |
            'Get client data' >> beam.ParDo(
                FilterPatient(args.patient))
        )

        # Running all defined extractors
        for extractor in extractors:
            extractor(
                "%s - patient %s" %
                (extractor.__name__, args.patient)
            ).extract(
                patient_data, args.output_folder
            )


if __name__ == '__main__':
    """Main function"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input_file', required=True,
                        help='Input csv file containing the data')
    parser.add_argument('-o', '--output_folder', default='results',
                        help='Output folder for the generated plots')
    parser.add_argument('-p', '--patient', required=True,
                        help='Patient that will analysed')

    run(parser.parse_args())
