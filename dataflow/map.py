import apache_beam as beam

output = []
def collect(row):
    output.append(row)
    
with beam.Pipeline() as P:
    numbers = P | beam.Create([1,2,3,4,5])
    squares = numbers | beam.Map(lambda x: x ** x)
    squares | beam.Map(collect)

print(output)