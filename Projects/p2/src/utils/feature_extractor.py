
from __future__ import absolute_import

import json
import logging
import pprint

import apache_beam as beam
import tensorflow as tf
import tensorflow_transform as tft

from apache_beam.io import filebasedsource
from tensorflow_transform.tf_metadata import dataset_schema
import time
from dateutil.parser import parse
import math

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

class FilterRows(beam.DoFn):

    def process(self, elem):
        if len(elem[1]['valid']) != 0:
            elem[1]['data'][0].append(elem[1]['valid'][0])
            return [(elem[1]['data'][0][2], elem[1]['data'][0])]

class UpdateSchema(beam.DoFn):

    def process(self, elem):
        items = elem[1]['items'][0]
        data = elem[1]['data']
        los = elem[1]['LoS']
        all_items_set = map(lambda x: x[0], items)
        items_measured = map(lambda x: x[4], data)
        overall_cgid = int(all(map(lambda x: x[13], data)))

        entry = {}
        counter = 0
        for idx, item in enumerate(all_items_set):
            try:
                i = items_measured.index(item)
                entry['item' + str(counter)] = float(data[i][9])
            except:
                entry['item' + str(counter)] = float(items[idx][1])
            counter+=1

        entry['LoS'] = los[0]
        entry['CGID'] = overall_cgid
        return [entry]

class MeanProcess(beam.DoFn):

    def process(self, elem):
        values = map(lambda v: v[1], elem[1])
        haids = map(lambda v: v[0], elem[1])
        mean = sum(values) / len(values)
        return [[elem[0], mean, haids]]


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



# [START dataflow_simple_feature_extraction]
class SimpleFeatureExtraction(beam.PTransform):
    """The feature extraction (element-wise transformations).
    We create a `PTransform` class. This `PTransform` is a bundle of
    transformations that can be applied to any other pipeline as a step.
    We'll extract all the raw features here. Due to the nature of `PTransform`s,
    we can only do element-wise transformations here. Anything that requires a
    full-pass of the data (such as feature scaling) has to be done with
    tf.Transform.
    """
    def __init__(self, source):
        super(SimpleFeatureExtraction, self).__init__()
        self.source = source

    def expand(self, p):
        data = (
                p
                | 'Read events' >> beam.io.ReadFromText(self.source, skip_header_lines=1)
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

        items_mean = (data
                      | 'Split Data' >> beam.Map(lambda event: (event.split(',')[4], (event.split(',')[2], float(event.split(',')[9]))))
                      | 'Group by Item' >> beam.GroupByKey()
                      | 'Calc Items Mean' >> beam.ParDo(MeanProcess())
                      | 'Make List' >> beam.combiners.ToList()
                      | 'Add key Items ' >> beam.ParDo(ItemsProcess())
                      )
        los_per_haid = (filtered_data
                        | 'Grouping by HAID' >> beam.GroupByKey()
                        | 'Calculate LoS' >> beam.ParDo(LosProcess())
                        )

        # Return the preprocessing pipeline.
        dataset = ({'data': filtered_data,
                    'items': items_mean,
                    'LoS': los_per_haid}
                   | 'Create Base Schema' >> beam.CoGroupByKey()
                   | 'Update Schema' >> beam.ParDo(UpdateSchema())
                   )
        return dataset
# [END dataflow_simple_feature_extraction]
