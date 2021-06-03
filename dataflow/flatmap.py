import apache_beam as beam

output = []
with beam.Pipeline() as P:
    ## Take various strings and split words from all those lists
    (
        P   
            | "Create" >> beam.Create(["This is List1", "This is List2"])
            | 'Split' >> beam.FlatMap(lambda x: x.split())
            | 'Collect' >> beam.Map(lambda row: output.append(row))
                        
    )

print(output)