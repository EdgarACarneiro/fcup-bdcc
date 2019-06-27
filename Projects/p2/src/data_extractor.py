import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import utils



def run(args):
    # Creating and opening the pipeline
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        filtered_data = utils.filter_data(p, args.input_file)

        _ = (
                filtered_data |
                'Get all Items' >> beam.Map(
            lambda event: (event[1][4], 0)) |
                'Group by Item' >> beam.GroupByKey() |
                'Count number of Items' >> beam.combiners.Count.Globally() |
                'Write to output file' >> beam.io.WriteToText(
            '%s/itemsCount.txt' % args.work_dir,
            shard_name_template='')
        )


if __name__ == '__main__':
    """Main function"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input_file', required=True,
                        help='Input csv file containing the data')
    parser.add_argument('-w', '--work_dir', default='tmp',
                        help='Output folder for the generated plots')

    run(parser.parse_args())
