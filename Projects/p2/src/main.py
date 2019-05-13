import apache_beam as beam
from beam.io import TextIO

#beam.io.Read(beam.io.TextFileSource('gs://big_events/EVENTS.csv.gz'), compression_type='GZIP')
#TextIO.Read.from('gs://big_events/EVENTS.csv.gz').withCompressionType(TextIO.CompressionType.GZIP)