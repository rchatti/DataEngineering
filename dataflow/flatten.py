import apache_beam as beam

output = []
def collect(row):
    output.append(row)

with beam.Pipeline() as P:
    a_names = P | 'a_names' >> beam.Create(['ant', 'apple', 'anthony'])
    b_names = P | 'b_names' >> beam.Create(['bharath'])

    ((a_names, b_names) 
    | beam.Flatten()
    | beam.Map(collect))

print(output)