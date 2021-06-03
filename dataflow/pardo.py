import apache_beam as beam 

class MultiplyByTen(beam.DoFn):
    def process(self, element):
        return [element ** 10]

output = []
def collect(row):
    output.append(row)
    return True

with beam.Pipeline() as P:
    (
        P 
        | "Create" >> beam.Create([1,2,3,4,5])
        | "ParDo Function" >> beam.ParDo(MultiplyByTen())
        | "Print" >> beam.Map(collect)
    )

print(output)