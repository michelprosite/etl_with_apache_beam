"""
Michel Souza Santana
Data Engineer
13/04/2024

Processo de ETL (Extract, Transform e Load) em um banco de dados Oracle usando o Apache Beam, Python e GCP.
"""
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

serviceAccount = r'/home/michel/Documentos/Projetos/keys/concise-vertex-420419-9fde64074908.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

def main(argv=None):
    options = PipelineOptions(
        flags=argv,
        project='concise-vertex-420419',
        runner='DataflowRunner',
        streaming=False,
        job_name='conection-postgres-for-parquet',
        temp_location='gs://etl-postgres-mss/temp',
        staging_locations='gs;//etl-postgres-mss/staging',
        template_location='gs://etl-postgres-mss/templates/conection-postgres-for-parquet',
        autoscaling_algorithm='THROUGHPUT_BASED',
        worker_machine_type='n1-standard-4',
        num_workers=1,
        max_num_workers=3,
        number_of_worker_harness_threads=2,
        #disk_size_gb=50,
        region='southamerica-east1',
        save_main_session=True,
        sdk_container_image='southamerica-east1-docker.pkg.dev/concise-vertex-420419/conection-postgres-for-parquet/postgres-dev:latest',
        sdk_location='container',
        requirements_file='./requirements.txt',
        metabase_file='./metadata.json',
        setup_file='./setup.py',
        service_account_email='teste-posteges-conection@concise-vertex-420419.iam.gserviceaccount.com',
    )

    from functions.get_names import GetNamesTables
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