from __future__ import absolute_import
import argparse
import math
import random
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


class UpdateSchema(beam.DoFn):

    def process(self, elem):
        items = elem[1]['items'][0]
        data = elem[1]['data']
        los = elem[1]['los']
        all_items_set = map(lambda x: x[0], items)
        items_measured = map(lambda x: x[4], data)
        overall_cgid = int(all(map(lambda x: x[13], data)))

        entry = []
        for idx, item in enumerate(all_items_set):
            try:
                i = items_measured.index(item)
                entry.append(float(data[i][9]))
            except:
                entry.append(float(items[idx][1]))

        entry.append(los[0])
        entry.append(overall_cgid)
        return [entry]


class FilterRows(beam.DoFn):

    def process(self, elem):
        if len(elem[1]['valid']) != 0:
            elem[1]['data'][0].append(elem[1]['valid'][0])
            return [(elem[1]['data'][0][2], elem[1]['data'][0])]


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
            yield (t[0], t[1] != u'')


class MeanProcess(beam.DoFn):

    def process(self, elem):
        values = map(lambda v: v[1], elem[1])
        haids = map(lambda v: v[0], elem[1])
        mean = sum(values)/len(values)
        return[[elem[0], mean, haids]]


class ItemsProcess(beam.DoFn):

    def process(self, elem):
        items_mean = map(lambda x: (x[0], x[1]), elem)
        haids = set()
        for val in elem:
            for haid in val[2]:
                if haid not in haids:
                    haids.add(haid)
                    yield (haid, items_mean)



class LosProcess(beam.DoFn):
    MS_TO_MIN = 1.0 / 3600.0

    def process(self, elem):
        dates_array = map(lambda arr: time.mktime(parse(arr[5]).timetuple()),
                          elem[1])

        return [(elem[0], (max(dates_array) - min(dates_array)) * self.MS_TO_MIN)]


def run(
        input_file,
        eval_percent=20.0,
        beam_options=None,
        work_dir=None):
    """Runs the whole preprocessing step.
    This runs the feature extraction PTransform, validates that the data conforms
    to the schema provided, normalizes the features, and splits the dataset into
    a training and evaluation dataset.
    """

    #     # Type checking
    # if not isinstance(labels, list):
    #     raise ValueError(
    #         '`labels` must be list(str). '
    #         'Given: {} {}'.format(labels, type(labels)))
    #


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

        # [START feature_extraction]
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
                         | 'Remove Non-Valid Rows' >> beam.ParDo(FilterRows()))

        items_mean = (filtered_data
                      | 'Split Data' >> beam.Map(lambda event: (event[1][4], (event[1][2], float(event[1][9]))))
                      | 'Group by Item' >> beam.GroupByKey()
                      | 'Calc Items Mean' >> beam.ParDo(MeanProcess())
                      | 'Make List' >> beam.combiners.ToList()
                      | 'Add key Items ' >> beam.ParDo(ItemsProcess())
                      #| 'Print Items Mean' >> beam.ParDo(CollectionPrinter())
                      )
        los_per_haid = (filtered_data
                        | 'Grouping by HAID' >> beam.GroupByKey()
                        | 'Calculate Los' >> beam.ParDo(LosProcess())
                        )

        dataset = ({'data': filtered_data,
                        'items': items_mean,
                        'los': los_per_haid}
                   | 'Create Base Schema' >> beam.CoGroupByKey()
                   | 'Update Schema' >> beam.ParDo(UpdateSchema())
                   | 'P Schema' >> beam.ParDo(CollectionPrinter())

                   )

        #[END feature_extraction]


        # [START_split_to_train_and_eval_datasets]
        # Split the dataset into a training set and an evaluation set
        # assert 0 < eval_percent < 100, 'eval_percent must in the range (0-100)'
        # train_dataset, eval_dataset = (
        #         dataset
        #         | 'Split dataset' >> beam.Partition(
        #     lambda elem, _: int(random.uniform(0, 100) < eval_percent), 2)
        # )
        # [END split_to_train_and_eval_datasets]



if __name__ == '__main__':
    """Main function"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input_file', required=True,
                        help='Input csv file containing the data')
    parser.add_argument('-o', '--work_dir', default='tmp',
                        help='Output folder for the generated plots')
    parser.add_argument('-p', '--total_items', required=True,
                        help='Number of features to be considered')

    args, pipeline_args = parser.parse_known_args()

    beam_options = PipelineOptions(pipeline_args, save_main_session=True)
    preprocess_data = run(
        args.input_file,
        beam_options=beam_options,
        work_dir=args.work_dir)

    # dump(preprocess_data, os.path.join(args.work_dir, 'PreprocessData'))
