import apache_beam as beam

output = []
def collect(row):
    output.append(row)
    return True

with beam.Pipeline() as P:
    (
        P 
        | 'Create' >> beam.Create(['Hello Beam'])
        | 'Print'  >> beam.Map(collect)
    )

print(output)