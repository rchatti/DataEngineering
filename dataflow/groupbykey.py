import apache_beam as beam 
"""
GroupByKey is:
- a Beam transform for processing collections of key/value pairs. 
- It’s a parallel reduction operation, analogous to the Shuffle phase of a Map/Shuffle/Reduce-style algorithm. 

The input to GroupByKey is: 
- a collection of key/value pairs that represents a multimap, 
    where the collection contains multiple pairs that have the same key, but different values. 
    
Given such a collection, you use GroupByKey to collect all of the values associated with each unique key.
"""
output = []
def collect(row):
    output.append(row)

def make_set(country):
    yield (country[0], country)

with beam.Pipeline() as P:
    (P
    | beam.Create(["America", "Australia", "Austria", 'Hyderabad'])
    ## | beam.Map(lambda x: (x[0], x))
    | beam.ParDo(make_set)
    | beam.GroupByKey()
    | beam.Map(collect))

print(output)