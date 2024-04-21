import psycopg2
import apache_beam as beam
import logging
import pandas as pd
from google.cloud import storage
import os
import json


class GetTables(beam.DoFn):
    def process(self, element):
        for table in element:
            # Parâmetros de conexão
            with open('config/conect_db_postgres.json') as f:
                config = json.load(f)

            # Definindo as variáveis de ambiente
            os.environ['DB_USER'] = config['DB_USER']
            os.environ['DB_PASSWORD'] = config['DB_PASSWORD']
            os.environ['DB_HOST'] = config['DB_HOST']
            os.environ['DB_PORT'] = config['DB_PORT']
            os.environ['DB_DATABASE'] = config['DB_DATABASE']
            
            #Pegando as credenciais via variável de ambiente
            USERNAME = os.getenv('DB_USER')
            PASSWORD = os.getenv('DB_PASSWORD')
            HOST = os.getenv('DB_HOST')
            PORT = os.getenv('DB_PORT')
            DATABASE = os.getenv('DB_DATABASE')

            conn_params = {
                "host": HOST,
                "database": DATABASE,
                "user": USERNAME,
                "password": PASSWORD,
                "port": PORT
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

                bucket_name = 'etl-postgres-mss'
                path = f'data/{table}.parquet'

                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blob = bucket.blob(path)

                # Converter DataFrame em string CSV
                csv_data = df.to_parquet(index=False)

                # Carregar a string CSV no bucket
                blob.upload_from_string(csv_data)

                logging.info(f'{table} {"=" * (80 - len(table))} {df.shape}')

                yield csv_data

            except psycopg2.Error as e:
                print("Erro ao conectar ou consultar o banco de dados:", e)
