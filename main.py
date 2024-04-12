import logging.config
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import os
import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

serviceAccount = r'/home/michel/Documentos/Projetos/keys/kei-1.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

def main(argv=None):
    options = PipelineOptions(
        flags=argv,
        project='teste-templates-420021',
        runner='DataflowRunner',
        temp_location='gs://postgres-etl/temp',
        staging_locations='gs;//postgres-etl/staging',
        region='southamerica-east1',
        save_main_session=True
    )

    from functions.hello_word import PrintHelloWorld

    with beam.Pipeline() as pipeline:
        hello = (
            pipeline
            | beam.Create([None])
            | beam.ParDo(PrintHelloWorld())
            #| beam.Map(print)
        )

        new_hello = (
            hello
            | f'new hello' >> beam.Map(print)
        )

if __name__ == '__main__':
    main()