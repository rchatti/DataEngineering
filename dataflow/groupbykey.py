import apache_beam as beam 

output = []
def collect(row):
    output.append(row)

def make_set(country):
    yield (country[0], country)

with beam.Pipeline() as P:
    (P
    | beam.Create(["America", "Australia", "Austria"])
    ## | beam.Map(lambda x: (x[0], x))
    | beam.ParDo(make_set)
    | beam.GroupByKey()
    | beam.Map(collect))

print(output)