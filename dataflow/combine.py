import apache_beam as beam

def combinefn(numbers):
    print(numbers)
    total = 0
    for num in numbers:
        total += num

    return total

with beam.Pipeline() as P:
    (P | beam.Create([1,2,3,4,5])
    | beam.CombineGlobally(combinefn))
