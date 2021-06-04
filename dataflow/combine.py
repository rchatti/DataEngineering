import apache_beam as beam

"""
Combine is a Beam transform for combining collections of elements or values in your data. 

- MUST provide the function that contains the logic for combining the elements or values. 
- The Combining function should be commutative and associative, 
    as the function is not necessarily invoked exactly once on all values with a given key. 
- the combining function might be called multiple times to perform partial combining on subsets of the value collection.

INPUT: LIST of values

"""

output = []
def collect(element):
    yield output.append(element)

def sum(values):
    # print(type(values))

    op_sum = 0
    for value in values: 
        op_sum += value
    return op_sum


with beam.Pipeline() as P:
    (
        P   | beam.Create([1,2,3,4,5, 6])
            | beam.CombineGlobally(sum)
            | beam.Map(collect)
    )

print(output)