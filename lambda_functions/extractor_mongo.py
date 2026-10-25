import json
import boto3
import pandas as pd
import os
from pymongo import MongoClient
from datetime import datetime

def lambda_handler(event, context):
    try:
        print("Iniciando extracción de telemetría desde MongoDB Atlas...")
        
        # 1. Conexión a MongoDB
        mongo_uri = os.environ['MONGO_URI']
        client = MongoClient(mongo_uri)
        db = client['fleetlogix_iot']
        collection = db['telemetria_gps']
        
        # 2. Extracción
        cursor = collection.find({})
        df_mongo = pd.DataFrame(list(cursor))
        
        # 3. Transformación básica y subida
        if not df_mongo.empty:
            df_final = df_mongo[['vehicle_id', 'timestamp', 'status']] # Columnas de ejemplo
            
            batch_id = int(datetime.now().timestamp())
            file_name = f"/tmp/telemetria_{batch_id}.csv"
            df_final.to_csv(file_name, index=False)
            
            s3 = boto3.client('s3')
            bucket_name = os.environ['S3_BUCKET_NAME']
            s3_key = f"raw/iot/telemetria_{batch_id}.csv"
            
            s3.upload_file(file_name, bucket_name, s3_key)
            
            return {
                'statusCode': 200,
                'body': json.dumps(f'Éxito: Telemetría subida a {s3_key}')
            }
        else:
            return {'statusCode': 204, 'body': 'No se encontraron datos nuevos.'}
            
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error en la ejecución: {str(e)}')
        }