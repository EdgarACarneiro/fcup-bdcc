from __future__ import absolute_import
from __future__ import print_function

import argparse
import json
import os
import sys
import tempfile

import apache_beam as beam
import tensorflow as tf

from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from tensorflow.python.framework import ops
import utils


class Predict(beam.DoFn):
    def __init__(self,
                 model_dir,
                 LoS_key,
                 meta_tag='serve',
                 meta_signature='predict',
                 meta_predictions='predictions'):
        super(Predict, self).__init__()
        self.model_dir = model_dir
        self.LoS_key = LoS_key
        self.meta_tag = meta_tag
        self.meta_signature = meta_signature
        self.meta_predictions = meta_predictions
        self.session = None
        self.graph = None
        self.feed_tensors = None
        self.fetch_tensors = None

    def process(self, inputs):
        # Create a session for every worker only once. The session is not
        # pickleable, so it can't be created at the DoFn constructor.
        if not self.session:
            self.graph = ops.Graph()
            with self.graph.as_default():
                self.session = tf.Session()
                metagraph_def = tf.compat.v1.saved_model.load(
                    self.session, {self.meta_tag}, self.model_dir)
            signature_def = metagraph_def.signature_def[self.meta_signature]

            # inputs
            self.feed_tensors = {
                k: self.graph.get_tensor_by_name(v.name)
                for k, v in signature_def.inputs.items()
            }

            # outputs/predictions
            self.fetch_tensors = {
                k: self.graph.get_tensor_by_name(v.name)
                for k, v in signature_def.outputs.items()
            }

        # Create a feed_dict for a single element.
        feed_dict = {
            tensor: [inputs[key]]
            for key, tensor in self.feed_tensors.items()
            if key in inputs
        }
        results = self.session.run(self.fetch_tensors, feed_dict)

        yield {
            'value': inputs[self.LoS_key],
            'predictions': results[self.meta_predictions][0].tolist()
        }


# [START dataflow_run_definition]
def run(model_dir, feature_extraction, sink, beam_options=None):
    print('Listening...')
    with beam.Pipeline(options=beam_options) as p:
        _ = (p
             | 'Feature extraction' >> feature_extraction
             | 'Predict' >> beam.ParDo(Predict(model_dir, 'LoS'))
             | 'Format as JSON' >> beam.Map(json.dumps)
             | 'Write predictions' >> sink)
# [END dataflow_run_definition]


if __name__ == '__main__':
    """Main function"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-w', '--work_dir', default='tmp',
                        help='Output folder for the generated plots')

    parser.add_argument(
        '-m',
        '--model-dir',
        required=True,
        help='Path to the exported TensorFlow model. '
             'This can be a Google Cloud Storage path.')

    verbs = parser.add_subparsers(dest='verb')
    batch_verb = verbs.add_parser('batch', help='Batch prediction')
    batch_verb.add_argument(
        '-i',
        '--input-file',
        required=True,
        help='Input file where data is read from. '
             'This can be a Google Cloud Storage path.')
    batch_verb.add_argument(
        '-o',
        '--outputs-dir',
        required=True,
        help='Directory to store prediction results. '
             'This can be a Google Cloud Storage path.')

    args, pipeline_args = parser.parse_known_args()

    beam_options = PipelineOptions(pipeline_args)
    beam_options.view_as(SetupOptions).save_main_session = True

    # [START dataflow batch]
    if args.verb == 'batch':
        results_prefix = os.path.join(args.outputs_dir, 'part')
        sink = beam.io.WriteToText(results_prefix)

    else:
        parser.print_usage()
        sys.exit(1)

    # [START dataflow_call_run]
    run(
        args.model_dir,
        utils.SimpleFeatureExtraction(args.input_file),
        sink,
        beam_options)
    # [END dataflow_call_run]
