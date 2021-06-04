
import apache_beam as beam
# from hello_world import collect

"""
FLATMAP: maps an element to multiple elements
"""

output = []

def collect(word):
    return output.append(word)

with beam.Pipeline() as P:
    ## Take various strings and split words from all those lists
    (
        P   | beam.Create(['Apache Beam', 'Google Cloud Dataflow'])
            | beam.FlatMap(lambda x: x.split())
            | beam.Map(collect)
    )

print(output)