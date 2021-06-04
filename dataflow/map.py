import apache_beam as beam

output = []
def collect(response):
    yield output.append(response)

with beam.Pipeline() as P:
    (
        P   |   beam.Create([1,2,3,4,5])
            |   beam.Map(lambda x: x * 5)
            |   beam.Map(collect)
    )

print(output)