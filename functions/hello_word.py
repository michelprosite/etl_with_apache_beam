import apache_beam as beam

class PrintHelloWorld(beam.DoFn):
        def process(self, element):
            element = 'Hello World'
            # print(element)
            yield element