import apache_beam as beam

"""
"""

output = []
def collect(element):
    yield output.append(element)

with beam.Pipeline() as P:
    (
        P   | beam.Create([(1,100), (2,100), (1, 200), (2, 300), (1,900), (1,100), (2, 200)])
            | beam.CombinePerKey(sum)
            | beam.Map(collect)
    )

print(output)