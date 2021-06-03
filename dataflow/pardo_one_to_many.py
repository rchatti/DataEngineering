import apache_beam as beam

class BreakIntoWordsDoFn(beam.DoFn):
    def process(self, element):
        return [element.split(' ')]


output = []
def collect(row):
    output.append(row)
    return True

with beam.Pipeline() as P:
    ## Map Each Input sentence into words tokenized by White Space

    (P
    | beam.Create(["Hello World,", "This is your Host RaviKishore"])
    | beam.ParDo(BreakIntoWordsDoFn())
    | beam.Map(collect)
    )

print(output)