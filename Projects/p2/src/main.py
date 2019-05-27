import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

"""Your task is to perform a statistical analysis on this data
and produce timeline graphs for each patient (SUBJECT_ID)"""

# https://medium.com/@brunoripa/apache-beam-a-python-example-5644ca4ed581
class Filter(beam.DoFn):
    """DoFn to filter the columns of interest to us"""

    def process(self, element):
        _, subject_id, hadm_id = element.split(",")


        return [{
            'patient': subject_id,
            'hospital_admission_id': hadm_id,
            'user': user
        }]

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

        csv_data = (
            p |
            'Reading Events' >> beam.io.ReadFromText(args.input_file, skip_header_lines=1)
        )

        # TODO: Hardcoded columns for now, later read Schema dinamically
        # -> Apache cant read csv's in fashion,  do it manually
        filtered_data = (
            csv_data | 
            'Get columns of interest' >> beam.ParDo(Filter())
        )

        #print_collection(csv_data)


if __name__ == '__main__':
    """Main function"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input_file', required=True,
                        help='Input csv file containing the data')
    parser.add_argument('-o', '--output_folder', default='',
                    help='Output folder for the generated plots')
    parser.add_argument('-p', '--patient', required=True,
                    help='Patient that will analysed')

    run(parser.parse_args())
