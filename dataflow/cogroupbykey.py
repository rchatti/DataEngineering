import apache_beam as beam

"""
fruits = ['apple', 'banana', 'cherry']
countries = ['argentina', 'brazil', 'canada']

Expected output = {'a':['apple', 'argentina']}
"""

output1 = []
op = []

class WordAlphabet:
    def __init__(self, alphabet, fruit, country):
        self.alphabet = alphabet
        self.fruit = fruit 
        self.country = country 

    def __str__(self):
        return "WordAlphabet(Alphabet: {}; Fruit: {}; Country: {})".format(self.alphabet, self.fruit, self.country)

output = []
def collect(row):
    output.append(row)

def cogbk_to_results(alphabetwords):
    (alphabet, words) = alphabetwords
    return WordAlphabet(alphabet, words['fruits'][0], words['countries'][0])

with beam.Pipeline() as P:
    fruits = P | 'Create Fruits' >> beam.Create(['apple', 'banana', 'cherry'])
    countries = P | 'Create Countries' >> beam.Create(['argentina', 'brazil', 'canada', 'checz'])

    fruit_sets = fruits | beam.Map(lambda fruit: (fruit[0], fruit))
    country_sets = countries | beam.Map(lambda country: [country[0], country])

    cogroup_data = {'fruits':fruit_sets, 'countries': country_sets} | beam.CoGroupByKey()
    cogroup_data | beam.Map(cogbk_to_results) 
    cogroup_data | beam.Map(collect)

print(output)