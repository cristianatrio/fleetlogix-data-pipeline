import os
from dotenv import load_dotenv
from pymongo import MongoClient
import random
from datetime import datetime, timedelta
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Cargar variables seguras
load_dotenv()
MONGO_URI = os.environ.get('MONGO_URI')

def generar_telemetria():
    logging.info("⏳ Conectando a MongoDB Atlas...")
    try:
        # Conectar al cluster
        client = MongoClient(MONGO_URI)
        
        # Crear base de datos y colección
        db = client['fleetlogix_iot']
        coleccion = db['telemetria_gps']
        
        # Limpiar datos viejos por si lo corrés varias veces
        coleccion.delete_many({})
        logging.info("🧹 Colección limpiada. Generando nuevos datos IoT...")

        vehiculos = [f"TRK-{i:03d}" for i in range(1, 201)] # 200 camiones
        datos_iot = []
        
        fecha_base = datetime.now() - timedelta(days=1)

        # Generar 10,000 pings de GPS aleatorios
        for _ in range(10000):
            ping = {
                "vehicle_id": random.choice(vehiculos),
                "timestamp": fecha_base + timedelta(minutes=random.randint(1, 1440)),
                "location": {
                    "type": "Point",
                    "coordinates": [
                        random.uniform(-58.5, -58.3), # Longitud (Aprox Buenos Aires)
                        random.uniform(-34.7, -34.5)  # Latitud
                    ]
                },
                "metrics": {
                    "speed_kmh": round(random.uniform(0, 90), 1),
                    "engine_temp_c": round(random.uniform(70, 105), 1),
                    "fuel_level_percent": round(random.uniform(10, 100), 1)
                },
                "status": random.choices(["moving", "idling", "stopped"], weights=[70, 20, 10])[0]
            }
            datos_iot.append(ping)

        # Inyectar en MongoDB
        coleccion.insert_many(datos_iot)
        logging.info(f"✅ ¡Éxito! Se insertaron {len(datos_iot)} registros de telemetría en MongoDB.")
        
    except Exception as e:
        logging.error(f"❌ Error conectando o insertando en Mongo: {e}")

if __name__ == "__main__":
    generar_telemetria()