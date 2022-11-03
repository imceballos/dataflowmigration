import apache_beam as beam

class SplitWords(beam.DoFn):
  def __init__(self, delimiter=' '):
    self.delimiter = delimiter

  def process(self, text):
      elements = {f"c{n}":word for n,word  in enumerate(text.split(self.delimiter))}
      yield elements


with beam.Pipeline() as pipeline:
    header = (pipeline
        | 'Read' >> beam.io.ReadFromText('test_files/inputexample.txt', skip_header_lines=5)
        | 'ConvertToDict' >> beam.ParDo(SplitWords())
        | 'Print' >> beam.Map(print))
