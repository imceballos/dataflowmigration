import apache_beam as beam

class SplitWords(beam.DoFn):
  def __init__(self, delimiter=' '):
    self.delimiter = delimiter

  def process(self, text):
      c1,c2,c3,c4,c5,c6 = text.split(self.delimiter)
      yield {"cod_vuelo": c1, "fecha": c2, "dow": c3}


class SplitDate(beam.DoFn):
  def __init__(self, choose='fecha'):
    self.choose = choose

  def process(self, text):
    yield text[self.choose][:5], text[self.choose][5:] 
    

with beam.Pipeline() as pipeline:
    header = (pipeline
        | 'Read' >> beam.io.ReadFromText('gs://dataflowexample3nov/inputexample.txt', skip_header_lines=5)
        | 'ConvertToDict' >> beam.ParDo(SplitWords())
        | 'SplitDates' >> beam.ParDo(SplitDate())
        | 'Write' >> beam.io.WriteToText('gs://dataflowexample3nov/fechas.txt'))
