import apache_beam as beam

class splitWords(beam.DoFn):
    def process(self, element):
        return element.split()

output = []
def collect(word):
    return output.append(word)

with beam.Pipeline() as P:
    (
        P   |   beam.Create(['Hello World!', 'This is your host Ravi'])
            |   beam.ParDo(splitWords())
            |   beam.Map(collect)
    )

print(output)