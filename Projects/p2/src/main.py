import apache_beam as beam
import tensorflow as tf

from tensorflow_transform.tf_metadata import dataset_schema
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

#beam.io.Read(beam.io.TextFileSource('gs://big_events/EVENTS.csv.gz'), compression_type='GZIP')
# TextIO.Read.from('gs://big_events/EVENTS.csv.gz').withCompressionType(TextIO.CompressionType.GZIP)

# INPUT_SCHEMA = {
#     # Features (inputs)
#     'RowID': dataset_schema.ColumnSchema(
#         tf.int64, [], dataset_schema.FixedColumnRepresentation()),
#     'SubjectID': dataset_schema.ColumnSchema(
#         tf.int64, [], dataset_schema.FixedColumnRepresentation()),
#     'HADM_ID': dataset_schema.ColumnSchema(
#         tf.int64, [], dataset_schema.FixedColumnRepresentation()),
#     'ICUSTAY_ID': dataset_schema.ColumnSchema(
#         tf.int64, [], dataset_schema.FixedColumnRepresentation()),
#     'ItemID': dataset_schema.ColumnSchema(
#         tf.int64, [], dataset_schema.FixedColumnRepresentation()),
#     'ChartTime': dataset_schema.ColumnSchema(
#         tf.timestamp, [], dataset_schema.FixedColumnRepresentation()),
#     'StoreTime': dataset_schema.ColumnSchema(
#         tf.timestamp, [], dataset_schema.FixedColumnRepresentation()),
#     'CGID': dataset_schema.ColumnSchema(
#         tf.int64, [], dataset_schema.FixedColumnRepresentation()),
#     'Value': dataset_schema.ColumnSchema(
#         tf.string, [], dataset_schema.FixedColumnRepresentation()),
#     'ValueNum': dataset_schema.ColumnSchema(
#         tf.int64, [], dataset_schema.FixedColumnRepresentation()),
#     'ValueUOM': dataset_schema.ColumnSchema(
#         tf.string, [], dataset_schema.FixedColumnRepresentation()),
#     'Warning': dataset_schema.ColumnSchema(
#         tf.int64, [], dataset_schema.FixedColumnRepresentation()),
#     'Error': dataset_schema.ColumnSchema(
#         tf.int64, [], dataset_schema.FixedColumnRepresentation()),

#     # Labels (outputs/predictions)
#     # 'Energy': dataset_schema.ColumnSchema(
#     #     tf.float32, [], dataset_schema.FixedColumnRepresentation()),
# }

# https://medium.com/@brunoripa/apache-beam-a-python-example-5644ca4ed581


class Split(beam.DoFn):

    # TODO: Change this to automatically read the schema and process it
    def process(self, element):
        # country, duration, user = element.split(",")
        print(element)

        # return [{
        #     'country': country,
        #     'duration': float(duration),
        #     'user': user
        # }]
        return element


class CollectionPrinter(beam.DoFn):
    """Helper DoFn able to print a PCollection contents"""

    def process(self, elem):
        print(elem)
        yield


def print_collection(collection):
    collection | beam.ParDo(CollectionPrinter())


def process(args):
    # Creating and opening the pipeline
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:

        csv_data = (
            p |
            'Reading Events' >> beam.io.ReadFromText(args.input_file, skip_header_lines=1)
        )

        print_collection(csv_data)

    # with beam.Pipeline() as p:

    #     rows = (
    #         p |
    #         beam.io.ReadFromText(args.input_file) |
    #         beam.ParDo(Split())
    #     )


if __name__ == '__main__':
    """Main function"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input_file', required=True,
                        help='Input csv file containing the data')

    process(parser.parse_args())
