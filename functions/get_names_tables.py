import psycopg2
import apache_beam as beam

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
            for table in tables:
                print(table[0])
            
            # Fechar o cursor e a conexão
            cursor.close()
            conn.close()

            yield tables
            
        except psycopg2.Error as e:
            print("Erro ao conectar ou consultar o banco de dados:", e)
            # Obter os resultados
            tables = cursor.fetchall()