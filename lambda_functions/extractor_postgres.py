import json
import boto3
import psycopg2
import pandas as pd
import os
from datetime import datetime

# En Lambda, las credenciales se cargan desde "Variables de Entorno" de la propia consola de AWS
def lambda_handler(event, context):
    try:
        print("Iniciando extracción desde PostgreSQL...")
        
        # 1. Conexión a Postgres
        conn = psycopg2.connect(
            host=os.environ['PG_HOST'],
            database=os.environ['PG_DATABASE'],
            user=os.environ['PG_USER'],
            password=os.environ['PG_PASSWORD'],
            port=os.environ['PG_PORT']
        )
        
        # 2. Extracción de datos
        query = "SELECT * FROM sales_data LIMIT 1000;" # Ejemplo simplificado
        df = pd.read_sql(query, conn)
        conn.close()
        
        # 3. Guardado temporal y subida a S3
        batch_id = int(datetime.now().timestamp())
        file_name = f"/tmp/postgres_data_{batch_id}.csv" # En Lambda solo se puede escribir en /tmp
        df.to_csv(file_name, index=False)
        
        s3 = boto3.client('s3')
        bucket_name = os.environ['S3_BUCKET_NAME']
        s3_key = f"raw/postgres/data_{batch_id}.csv"
        
        s3.upload_file(file_name, bucket_name, s3_key)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Éxito: Datos subidos a S3 en {s3_key}')
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error en la ejecución: {str(e)}')
        }