"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma (Pandas), Guarda en AWS S3 (Data Lake)
y Carga masiva en Snowflake vía COPY INTO.
"""
import os
import csv
from dotenv import load_dotenv
load_dotenv()
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import psycopg2
import snowflake.connector
import boto3
import pandas as pd
from pymongo import MongoClient
import numpy as np
from datetime import datetime, timedelta
import logging
import schedule
import time
import json

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones
POSTGRES_CONFIG = {
    'host': 'localhost',
    'database': 'fleetlogix',
    'user': 'postgres',
    'password': os.environ['PG_PASSWORD'],
    'port': 5432
}

SNOWFLAKE_CONFIG = {
    'user': 'test02',
    'account': 'SLKKNYO-JH52288',
    'private_key_path': './.secrets/rsa_key.p8',
    'warehouse': 'FLEETLOGIX_WH',
    'database': 'FLEETLOGIX_DW',
    'schema': 'ANALYTICS',
    'role': 'ETL_ROLE'
}

# --- NUEVA CONFIGURACIÓN AWS (SEGURA) ---
AWS_CONFIG = {
    'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),         
    'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),     
    'bucket_name': 'fleetlogix-datalake-catrio'   
}

class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.mongo_uri = os.environ.get('MONGO_URI')
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }

    def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info(" Conectado a PostgreSQL")

            with open(SNOWFLAKE_CONFIG['private_key_path'], "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=None,
                    backend=default_backend()
                )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            self.sf_conn = snowflake.connector.connect(
                user=SNOWFLAKE_CONFIG['user'],
                account=SNOWFLAKE_CONFIG['account'],
                private_key=pkb,
                warehouse=SNOWFLAKE_CONFIG['warehouse'],
                database=SNOWFLAKE_CONFIG['database'],
                schema=SNOWFLAKE_CONFIG['schema'],
                role=SNOWFLAKE_CONFIG['role']
            )
            logging.info(" Conectado a Snowflake")
            return True
        except Exception as e:
            logging.error(f" Error en conexión: {e}")
            return False

    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos del día anterior de PostgreSQL"""
        logging.info(" Iniciando extracción de datos...")
        query = """
        SELECT
            d.delivery_id, d.trip_id, d.tracking_number, d.customer_name,
            d.delivery_address, d.package_weight_kg, d.scheduled_datetime,
            d.delivered_datetime, d.delivery_status, d.recipient_signature,
            t.vehicle_id, t.driver_id, t.route_id, t.departure_datetime,
            t.arrival_datetime, t.fuel_consumed_liters, r.distance_km,
            r.toll_cost, r.destination_city
        FROM deliveries d
        JOIN trips t ON d.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        """
        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f" Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f" Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info(" Iniciando transformación de datos...")
        try:
            df['delivery_time_minutes'] = ((pd.to_datetime(df['delivered_datetime']) - pd.to_datetime(df['scheduled_datetime'])).dt.total_seconds() / 60).round(2)
            df['delay_minutes'] = df['delivery_time_minutes'].apply(lambda x: max(0, x) if x > 0 else 0)
            df['is_on_time'] = df['delay_minutes'] <= 30
            df['trip_duration_hours'] = ((pd.to_datetime(df['arrival_datetime']) - pd.to_datetime(df['departure_datetime'])).dt.total_seconds() / 3600).round(2)

            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)
            df['deliveries_per_hour'] = (df['deliveries_in_trip'] / df['trip_duration_hours']).round(2)

            df['fuel_efficiency_km_per_liter'] = (df['distance_km'] / df['fuel_consumed_liters']).round(2)
            df['cost_per_delivery'] = ((df['fuel_consumed_liters'] * 5000 + df['toll_cost']) / df['deliveries_in_trip']).round(2)
            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)

            df = df[df['delivery_time_minutes'] >= 0]
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)]

            df['valid_from'] = pd.to_datetime(df['scheduled_datetime']).dt.date
            df['valid_to'] = pd.to_datetime('2099-12-31')
            df['is_current'] = True

            self.metrics['records_transformed'] = len(df)
            logging.info(f" Transformados {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f" Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    def create_tables_if_not_exist(self):
        """Crear tablas en Snowflake si no existen"""
        cursor = self.sf_conn.cursor()
        tables = {
            "ANALYTICS.DIM_CUSTOMER": """
                CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_CUSTOMER (
                    customer_key        NUMBER AUTOINCREMENT PRIMARY KEY,
                    customer_name       VARCHAR(255),
                    customer_type       VARCHAR(50),
                    city                VARCHAR(100),
                    first_delivery_date DATE,
                    total_deliveries    NUMBER DEFAULT 0,
                    customer_category   VARCHAR(50)
                )
            """,
            "ANALYTICS.DIM_DRIVER": """
                CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_DRIVER (
                    driver_key  NUMBER AUTOINCREMENT PRIMARY KEY,
                    driver_id   NUMBER,
                    valid_from  DATE,
                    valid_to    DATE,
                    is_current  BOOLEAN DEFAULT TRUE
                )
            """,
            "ANALYTICS.FACT_DELIVERIES": """
                CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_DELIVERIES (
                    fact_id                      NUMBER AUTOINCREMENT PRIMARY KEY,
                    date_key                     NUMBER,
                    scheduled_time_key           NUMBER,
                    delivered_time_key           NUMBER,
                    vehicle_key                  NUMBER,
                    driver_key                   NUMBER,
                    route_key                    NUMBER,
                    customer_key                 NUMBER,
                    delivery_id                  NUMBER,
                    trip_id                      NUMBER,
                    tracking_number              VARCHAR(100),
                    package_weight_kg            FLOAT,
                    distance_km                  FLOAT,
                    fuel_consumed_liters         FLOAT,
                    delivery_time_minutes        FLOAT,
                    delay_minutes                FLOAT,
                    deliveries_per_hour          FLOAT,
                    fuel_efficiency_km_per_liter FLOAT,
                    cost_per_delivery            FLOAT,
                    revenue_per_delivery         FLOAT,
                    is_on_time                   BOOLEAN,
                    is_damaged                   BOOLEAN,
                    has_signature                BOOLEAN,
                    delivery_status              VARCHAR(50),
                    etl_batch_id                 NUMBER
                )
            """,
            "ANALYTICS.FACT_TELEMETRY": """
                CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_TELEMETRY (
                    telemetry_id    NUMBER AUTOINCREMENT PRIMARY KEY,
                    vehicle_id      VARCHAR(50),
                    timestamp       TIMESTAMP_NTZ,
                    latitude        FLOAT,
                    longitude       FLOAT,
                    speed_kmh       FLOAT,
                    engine_temp     FLOAT,
                    status          VARCHAR(50),
                    etl_batch_id    NUMBER
                )
            """,
        }
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS ANALYTICS")
            self.sf_conn.commit()
        except Exception as e:
            logging.warning(f" No se pudo crear schema ANALYTICS: {e}")

        for table_name, ddl in tables.items():
            try:
                cursor.execute(ddl)
                self.sf_conn.commit()
                logging.info(f" Tabla {table_name} verificada/creada")
            except Exception as e:
                logging.warning(f" Aviso en tabla {table_name}: {e}")

    def load_dimensions(self, df: pd.DataFrame):
        """Cargar o actualizar dimensiones usando Bulk INSERT"""
        logging.info(" Cargando dimensiones...")
        cursor = self.sf_conn.cursor()

        try:
            customers_df = df[['customer_name', 'destination_city']].drop_duplicates(subset=['customer_name'])
            customer_data = [(row['customer_name'], row['destination_city']) for _, row in customers_df.iterrows()]

            if customer_data:
                cursor.executemany("""
                    INSERT INTO ANALYTICS.DIM_CUSTOMER
                    (customer_name, customer_type, city, first_delivery_date, total_deliveries, customer_category)
                    VALUES (%s, 'Individual', %s, CURRENT_DATE(), 0, 'Regular')
                """, customer_data)

            driver_ids = df['driver_id'].dropna().unique().tolist()
            if driver_ids:
                placeholders = ','.join(['%s'] * len(driver_ids))
                cursor.execute(f"""
                    UPDATE ANALYTICS.DIM_DRIVER
                    SET valid_to = CURRENT_DATE() - 1, is_current = FALSE
                    WHERE driver_id IN ({placeholders}) AND is_current = TRUE
                """, driver_ids)

            self.sf_conn.commit()
            logging.info(" Dimensiones actualizadas correctamente")
        except Exception as e:
            logging.error(f" Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1

    def load_facts(self, df: pd.DataFrame):
        """Guardar hechos en CSV, subir a AWS S3 (Data Lake) y cargar a Snowflake vía COPY INTO"""
        logging.info(" Iniciando proceso de Data Lake (S3) y Data Warehouse (Snowflake)...")
        cursor = self.sf_conn.cursor()

        try:
            # 1. Preparar datos
            fact_data = []
            for _, row in df.iterrows():
                date_key = int(pd.to_datetime(row['scheduled_datetime']).strftime('%Y%m%d'))
                scheduled_time_key = pd.to_datetime(row['scheduled_datetime']).hour * 100
                delivered_time_key = pd.to_datetime(row['delivered_datetime']).hour * 100

                fact_data.append((
                    date_key, scheduled_time_key, delivered_time_key,
                    row['vehicle_id'], row['driver_id'], row['route_id'], 1,
                    row['delivery_id'], row['trip_id'], row['tracking_number'],
                    row['package_weight_kg'], row['distance_km'], row['fuel_consumed_liters'],
                    row['delivery_time_minutes'], row['delay_minutes'], row['deliveries_per_hour'],
                    row['fuel_efficiency_km_per_liter'], row['cost_per_delivery'], row['revenue_per_delivery'],
                    row['is_on_time'], False, row['recipient_signature'], row['delivery_status'], self.batch_id
                ))

            # 2. Guardar en CSV local temporal
            csv_filename = f"fact_deliveries_{self.batch_id}.csv"
            logging.info(f" Creando archivo local temporal: {csv_filename}")
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(fact_data)

            # 3. Subir el archivo a AWS S3 (Data Lake)
            logging.info(" Subiendo archivo a AWS S3...")
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_CONFIG['aws_access_key_id'],
                aws_secret_access_key=AWS_CONFIG['aws_secret_access_key']
            )
            s3_path = f"raw/facts/{csv_filename}"
            s3_client.upload_file(csv_filename, AWS_CONFIG['bucket_name'], s3_path)
            logging.info(" Archivo subido a S3 exitosamente")

            # 4. COPY INTO desde S3 a Snowflake (carga masiva nativa)
            logging.info(" Ejecutando COPY INTO en Snowflake...")
            copy_query = f"""
                COPY INTO ANALYTICS.FACT_DELIVERIES (
                    date_key, scheduled_time_key, delivered_time_key, vehicle_key, driver_key,
                    route_key, customer_key, delivery_id, trip_id, tracking_number, package_weight_kg,
                    distance_km, fuel_consumed_liters, delivery_time_minutes, delay_minutes,
                    deliveries_per_hour, fuel_efficiency_km_per_liter, cost_per_delivery,
                    revenue_per_delivery, is_on_time, is_damaged, has_signature, delivery_status, etl_batch_id
                )
                FROM 's3://{AWS_CONFIG['bucket_name']}/{s3_path}'
                CREDENTIALS=(AWS_KEY_ID='{AWS_CONFIG['aws_access_key_id']}' AWS_SECRET_KEY='{AWS_CONFIG['aws_secret_access_key']}')
                FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
            """
            cursor.execute(copy_query)
            self.sf_conn.commit()

            # 5. Limpiar archivo temporal local
            if os.path.exists(csv_filename):
                os.remove(csv_filename)

            self.metrics['records_loaded'] = len(fact_data)
            logging.info(f" Cargados {len(fact_data)} registros en Snowflake en tiempo récord")

        except Exception as e:
            logging.error(f" Error en carga Data Lake / Snowflake: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1

    def extract_mongodb_telemetry(self) -> pd.DataFrame:
        """Extrae datos no estructurados de IoT desde MongoDB Atlas"""
        logging.info("⏳ Extrayendo telemetría IoT desde MongoDB...")
        try:
            client = MongoClient(self.mongo_uri)
            db = client['fleetlogix_iot']
            collection = db['telemetria_gps']
            
            # Traemos todos los documentos de la colección
            cursor = collection.find({})
            df_mongo = pd.DataFrame(list(cursor))
            
            if not df_mongo.empty:
                # Transformación rápida: Aplanar el JSON de MongoDB
                df_mongo['latitude'] = df_mongo['location'].apply(lambda x: x['coordinates'][1])
                df_mongo['longitude'] = df_mongo['location'].apply(lambda x: x['coordinates'][0])
                df_mongo['speed_kmh'] = df_mongo['metrics'].apply(lambda x: x['speed_kmh'])
                df_mongo['engine_temp'] = df_mongo['metrics'].apply(lambda x: x['engine_temp_c'])
                
                # Limpiamos columnas innecesarias
                df_final = df_mongo[['vehicle_id', 'timestamp', 'latitude', 'longitude', 'speed_kmh', 'engine_temp', 'status']].copy()
                logging.info(f"✅ {len(df_final)} registros de telemetría extraídos y aplanados.")
                return df_final
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"❌ Error en extracción MongoDB: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    def load_telemetry_facts(self, df: pd.DataFrame):
        """Guardar telemetría en CSV, subir a S3 y cargar a Snowflake"""
        logging.info(" Iniciando carga de Telemetría (S3 -> Snowflake)...")
        cursor = self.sf_conn.cursor()
        
        try:
            # 1. Preparar datos
            df['etl_batch_id'] = self.batch_id
            
            # 2. Guardar en CSV local temporal (sin header para Snowflake)
            csv_filename = f"telemetria_{self.batch_id}.csv"
            df.to_csv(csv_filename, index=False, header=False)
            
            # 3. Subir a AWS S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_CONFIG['aws_access_key_id'],
                aws_secret_access_key=AWS_CONFIG['aws_secret_access_key']
            )
            s3_path = f"raw/iot/{csv_filename}"
            s3_client.upload_file(csv_filename, AWS_CONFIG['bucket_name'], s3_path)
            logging.info(f"☁️ Archivo {csv_filename} subido a S3 en carpeta iot")
            
            # 4. COPY INTO desde S3 a Snowflake
            logging.info("❄️ Ejecutando COPY INTO en Snowflake para FACT_TELEMETRY...")
            copy_query = f"""
                COPY INTO ANALYTICS.FACT_TELEMETRY (
                    vehicle_id, timestamp, latitude, longitude, speed_kmh, engine_temp, status, etl_batch_id
                )
                FROM 's3://{AWS_CONFIG['bucket_name']}/{s3_path}'
                CREDENTIALS=(AWS_KEY_ID='{AWS_CONFIG['aws_access_key_id']}' AWS_SECRET_KEY='{AWS_CONFIG['aws_secret_access_key']}')
                FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
            """
            cursor.execute(copy_query)
            self.sf_conn.commit()
            
            # 5. Limpiar archivo temporal local
            if os.path.exists(csv_filename):
                os.remove(csv_filename)
                
            self.metrics['records_loaded'] += len(df)
            logging.info(f"✅ Cargados {len(df)} registros de telemetría en Snowflake.")
            
        except Exception as e:
            logging.error(f"❌ Error en carga Telemetría: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1

    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f" Iniciando ETL - Batch ID: {self.batch_id}")
        try:
            if not self.connect_databases():
                return
            self.create_tables_if_not_exist()
            
            # 1. FLUJO TRADICIONAL (Postgres)
            df = self.extract_daily_data()
            if not df.empty:
                df_transformed = self.transform_data(df)
                if not df_transformed.empty:
                    self.load_dimensions(df_transformed)
                    self.load_facts(df_transformed)
            
            # 2. FLUJO MODERNO (MongoDB)
            df_telemetria = self.extract_mongodb_telemetry()
            if not df_telemetria.empty:
                self.load_telemetry_facts(df_telemetria)
                
            self._calculate_daily_totals()
            self.close_connections()
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f" ETL completado en {duration:.2f} segundos")
            logging.info(f" Métricas: {json.dumps(self.metrics, indent=2)}")
        except Exception as e:
            logging.error(f" Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()

    def _calculate_daily_totals(self):
        cursor = self.sf_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ANALYTICS.DAILY_TOTALS (
                    batch_id NUMBER, total_records NUMBER, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                );
            """)
            cursor.execute("""
                INSERT INTO ANALYTICS.DAILY_TOTALS (batch_id, total_records)
                SELECT %s, COUNT(*) FROM ANALYTICS.FACT_DELIVERIES;
            """, (self.batch_id,))
            self.sf_conn.commit()
            logging.info(" Totales diarios calculados")
        except Exception as e:
            logging.error(f" Error calculando totales: {e}")

    def close_connections(self):
        if self.pg_conn: self.pg_conn.close()
        if self.sf_conn: self.sf_conn.close()
        logging.info(" Conexiones cerradas")

def job():
    etl = FleetLogixETL()
    etl.run_etl()

def main():
    logging.info(" Pipeline ETL FleetLogix iniciado")
    schedule.every().day.at("02:00").do(job)
    logging.info(" ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")
    job()
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
