import logging.config
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import os
import logging
import sys
import psycopg2
import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

#serviceAccount = r'/home/michel/Documentos/Projetos/keys/key-1.json'
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

def main(argv=None):
    options = PipelineOptions(
        flags=argv,
        project='teste-templates-420021',
        runner='DataflowRunner',
        temp_location='gs://etl-postgres-for-parquet/temp',
        staging_locations='gs;//etl-postgres-for-parquet/staging',
        region='southamerica-east1',
        save_main_session=True,
        #requirements_file='/home/michel/Documentos/Projetos/etl_with_apache_beam/requirements.txt'
    )

    from functions.get_names_tables import GetNamesTables
    from functions.get_tbles import GetTables   
                

    with beam.Pipeline(options=options) as pipeline:
        get_names = (
            pipeline
            | f'Create names tables' >> beam.Create([None])
            | f'Get Names' >> beam.ParDo(GetNamesTables())
        )

        get_tables = (
            get_names
            | f'Create Get Tables' >> beam.ParDo(GetTables())
        )

if __name__ == '__main__':
    main()