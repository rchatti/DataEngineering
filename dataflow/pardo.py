import apache_beam as beam
from apache_beam.transforms.core import ParDo

output = []
def collect(numbers):
    output.append(numbers)

def multBy10fn(in_element):
    yield in_element * 10

with beam.Pipeline() as P:
    (P   | 'Create' >> beam.Create([1,2,3,4,5])
        | 'Multiply' >> beam.ParDo(multBy10fn)
        | 'Collect' >> beam.Map(collect)
        )

print(output)