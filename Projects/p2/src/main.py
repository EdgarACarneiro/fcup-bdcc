import apache_beam as beam
import argparse

#beam.io.Read(beam.io.TextFileSource('gs://big_events/EVENTS.csv.gz'), compression_type='GZIP')
# TextIO.Read.from('gs://big_events/EVENTS.csv.gz').withCompressionType(TextIO.CompressionType.GZIP)


class Split(beam.DoFn):

    # TODO: Change this to automatically read the schema and process it
    def process(self, element):
        country, duration, user = element.split(",")

        return [{
            'country': country,
            'duration': float(duration),
            'user': user
        }]

def process(args):
    with beam.Pipeline() as p:

        rows = (
            p |
            beam.io.ReadFromText(args.input_file) |
            beam.ParDo(Split())
        )

if __name__ == '__main__':
    """Main function"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input_file', required=True,
                        help='Input csv file containing the data')

    process(parser.parse_args)
