import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromSQL

# Defina as opções do pipeline
pipeline_options = PipelineOptions()

# Defina as informações de conexão ao banco de dados PostgreSQL
connection_config = {
    "drivername": "postgresql+psycopg2",
    "host": "159.223.187.110",
    "port": "5432",
    "username": "etlreadonly",
    "password": "novadrive376A@",
    "database": "novadrive"
}

# Defina a consulta SQL
sql_query = "SELECT * FROM nome_da_tabela LIMIT 10"

# Crie o pipeline
with beam.Pipeline(options=pipeline_options) as p:
    # Crie uma transformação ReadFromSQL
    result = p | ReadFromSQL(
        query=sql_query,
        connection_config=connection_config
    )
    
    # Exiba os resultados
    result | beam.Map(print)
