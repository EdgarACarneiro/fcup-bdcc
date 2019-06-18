from __future__ import absolute_import
import argparse
import math
import tempfile
import os
import apache_beam as beam
import tensorflow as tf
from tensorflow_transform.beam.tft_beam_io import transform_fn_io
from apache_beam.options.pipeline_options import PipelineOptions
import tensorflow_transform.beam.impl as beam_impl
from dateutil.parser import parse
import time


class CollectionPrinter(beam.DoFn):
    """Helper DoFn able to print a PCollection contents"""

    def process(self, elem):
        print(elem)


class FilterRows(beam.DoFn):

    def process(self, elem):
        if len(elem[1]['valid']) != 0:
            elem[1]['data'][0].append(elem[1]['valid'][0])
            return [elem[1]['data'][0]]


class ValidRows(beam.DoFn):
    MS_TO_MIN = 1.0 / 3600.0
    HOUR_TO_MIN = 60

    def process(self, tup):
        dates_array = map(lambda r: time.mktime(parse(r[5]).timetuple()), tup[1])

        minDate = min(dates_array) * self.MS_TO_MIN
        maxDate = minDate + 24 * self.HOUR_TO_MIN

        filtered_times = filter(lambda r:
                                int(math.floor(minDate)) < time.mktime(parse(r[5]).timetuple()) * self.MS_TO_MIN < int(
                                    math.ceil(maxDate)),
                                tup[1])

        valid_rows = map(lambda r: (r[0], r[7]), filtered_times)

        for t in valid_rows:
            yield (t[0], True if t[1] != u'' else False)


class LosProcess(beam.DoFn):
    MS_TO_MIN = 1.0 / 3600.0

    def process(self, haid_entry):
        dates_array = map(lambda date: time.mktime(parse(date).timetuple()),
                          haid_entry[1])

        return [(haid_entry[0], (max(dates_array) - min(dates_array)) * self.MS_TO_MIN)]


def run(
        input_file,
        feature_scaling=None,
        eval_percent=20.0,
        beam_options=None,
        work_dir=None):
    """Runs the whole preprocessing step.
    This runs the feature extraction PTransform, validates that the data conforms
    to the schema provided, normalizes the features, and splits the dataset into
    a training and evaluation dataset.
    """

    # Populate optional arguments
    if not feature_scaling:
        feature_scaling = lambda inputs: inputs

    #     # Type checking
    # if not isinstance(labels, list):
    #     raise ValueError(
    #         '`labels` must be list(str). '
    #         'Given: {} {}'.format(labels, type(labels)))
    #
    # if not isinstance(feature_extraction, beam.PTransform):
    #     raise ValueError(
    #         '`feature_extraction` must be {}. '
    #         'Given: {} {}'.format(beam.PTransform,
    #                               feature_extraction, type(feature_extraction)))

    if not callable(feature_scaling):
        raise ValueError(
            '`feature_scaling` must be callable. '
            'Given: {} {}'.format(feature_scaling,
                                  type(feature_scaling)))

    if beam_options and not isinstance(beam_options, PipelineOptions):
        raise ValueError(
            '`beam_options` must be {}. '
            'Given: {} {}'.format(PipelineOptions,
                                  beam_options, type(beam_options)))

    if not work_dir:
        work_dir = tempfile.mkdtemp(prefix='tensorflow-preprocessing')

    tft_temp_dir = os.path.join(work_dir, 'tft-temp')
    train_dataset_dir = os.path.join(work_dir, 'train-dataset')
    eval_dataset_dir = os.path.join(work_dir, 'eval-dataset')

    transform_fn_dir = os.path.join(work_dir, transform_fn_io.TRANSFORM_FN_DIR)
    if tf.gfile.Exists(transform_fn_dir):
        tf.gfile.DeleteRecursively(transform_fn_dir)

    with beam.Pipeline(options=beam_options) as p, \
            beam_impl.Context(temp_dir=tft_temp_dir):

        # Transform and validate the input data matches the input schema
        data = (
                p
                | 'Read events' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
        )
        filter_rows = (data
                       | 'Get columns of interest' >> beam.FlatMap(lambda event: [(event.split(',')[1],
                                                                                   event.split(','))])
                       | 'Grouping by Patient' >> beam.GroupByKey()
                       | 'Get valid Rows' >> beam.ParDo(ValidRows())
                       )

        data_tuple = (data | 'Make Tuple' >> beam.FlatMap(lambda event: [(event.split(',')[0], event.split(','))]))

        filtered_data = ({'data': data_tuple,
                          'valid': filter_rows}
                         | 'Join Datasets' >> beam.CoGroupByKey()
                         | 'Remove Non-Valid Rows' >> beam.ParDo(FilterRows())
                         | 'Print Collection' >> beam.ParDo(CollectionPrinter()))

        # items_mean = (data
        #               | 'Split Data' >> beam.Map(lambda event: (int(event.split(',')[4]), float(event.split(',')[9])))
        #               | 'Group by Item' >> beam.GroupByKey()
        #               | 'Calc Items Mean' >> beam.Map(lambda t: (t[0], sum(t[1]) / len(t[1])))
        #               )
        #
        # los_per_haid = (data
        #                 | 'Get columns of interest' >> beam.FlatMap(
        #             lambda event: [(event.split(',')[2], event.split(',')[5])])
        #                 | 'Grouping by HAID' >> beam.GroupByKey()
        #                 | 'Calculate Los' >> beam.ParDo(LosProcess()))


if __name__ == '__main__':
    """Main function"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input_file', required=True,
                        help='Input csv file containing the data')
    parser.add_argument('-o', '--work_dir', default='tmp',
                        help='Output folder for the generated plots')

    args, pipeline_args = parser.parse_known_args()

    beam_options = PipelineOptions(pipeline_args, save_main_session=True)
    preprocess_data = run(
        args.input_file,
        # feature_scaling=preprocessor.normalize_inputs,
        beam_options=beam_options,
        work_dir=args.work_dir)

    # dump(preprocess_data, os.path.join(args.work_dir, 'PreprocessData'))
