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

serviceAccount = r'/home/michel/Documentos/Projetos/keys/key-1.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

def main(argv=None):
    options = PipelineOptions(
        flags=argv,
        project='teste-templates-420021',
        runner='DataflowRunner',
        temp_location='gs://postgres-etl/temp',
        staging_locations='gs;//postgres-etl/staging',
        region='southamerica-east1',
        save_main_session=True,
        requirements_file='/home/michel/Documentos/Projetos/etl_with_apache_beam/requirements.txt'
    )

    class GetNamesTables(beam.DoFn):
        def process(self, element):
            element = None
            # Parâmetros de conexão
            conn_params = {
                "host": "159.223.187.110",
                "database": "novadrive",
                "user": "etlreadonly",
                "password": "novadrive376A@",
                "port": "5432"
            }

            try:
                # Conectar ao banco de dados
                conn = psycopg2.connect(**conn_params)
                
                # Criar um cursor para executar consultas SQL
                cursor = conn.cursor()
                
                # Consulta SQL para obter os nomes das tabelas existentes
                query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'  -- Onde 'public' é o esquema padrão do PostgreSQL
                """
                
                # Executar a consulta
                cursor.execute(query)
                
                # Obter os resultados
                tables = cursor.fetchall()
                
                # Exibir os nomes das tabelas
                list_names = []
                for table in tables:
                    list_names.append(table[0])
                
                # Fechar o cursor e a conexão
                cursor.close()
                conn.close()

                yield list_names
                
            except psycopg2.Error as e:
                print("Erro ao conectar ou consultar o banco de dados:", e)
                # Obter os resultados
                tables = cursor.fetchall()

    def printf(element):
        for table in element:
            # Parâmetros de conexão
            conn_params = {
                "host": "159.223.187.110",
                "database": "novadrive",
                "user": "etlreadonly",
                "password": "novadrive376A@",
                "port": "5432"
            }

            try:
                # Conectar ao banco de dados
                conn = psycopg2.connect(**conn_params)
                
                # Criar um cursor para executar consultas SQL
                cursor = conn.cursor()
                
                # Consulta SQL para obter os nomes das tabelas existentes
                query = f"SELECT * FROM {table} LIMIT 10"
                
                # Executar a consulta
                cursor.execute(query)
                
                # Obter os nomes das colunas
                col_names = [desc[0] for desc in cursor.description]

                # Obter os resultados
                rows = cursor.fetchall()
                
                # Criar DataFrame usando os nomes das colunas
                df = pd.DataFrame(rows, columns=col_names)
                
                # Fechar o cursor e a conexão
                cursor.close()
                conn.close()

                bucket_name = 'postgres-etl'
                path = f'raw/{table}.csv'

                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blob = bucket.blob(path)

                # Converter DataFrame em string CSV
                csv_data = df.to_csv(index=False)

                # Carregar a string CSV no bucket
                blob.upload_from_string(csv_data)

                logging.info(f'{table} {"=" * (80 - len(table))} {df.shape}')

                yield csv_data

            except psycopg2.Error as e:
                print("Erro ao conectar ou consultar o banco de dados:", e)

                

    with beam.Pipeline(options=options) as pipeline:
        get_names = (
            pipeline
            | f'Create names tables' >> beam.Create([None])
            | f'Get Names' >> beam.ParDo(GetNamesTables())
        )

        get_tables = (
            get_names
            | f'Create Get Tables' >> beam.ParDo(printf)
        )

if __name__ == '__main__':
    main()