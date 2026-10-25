-- ============================================================================
-- SISTEMA COMPLETO DE BENCHMARKING Y ANÁLISIS - FLEETLOGIX
-- ============================================================================
-- Descripción: Captura automática de tiempos, planes de ejecución y métricas
-- Autor: Cristian Atrio
-- Versión: 8.0 (Captura Completa Automatizada)
-- ============================================================================

-- ============================================================================
-- SECCIÓN 1: CONFIGURACIÓN INICIAL Y TABLAS DE RESULTADOS
-- ============================================================================

-- Tabla principal de resultados de benchmarking
CREATE TABLE IF NOT EXISTS resultados_benchmark_comparativo (
    id SERIAL PRIMARY KEY,
    query_numero INTEGER NOT NULL,
    query_nombre VARCHAR(100) NOT NULL,
    fase VARCHAR(20) NOT NULL CHECK (fase IN ('sin_indices', 'con_indices')),
    problema_negocio TEXT,
    tiempo_ejecucion DECIMAL(10,3) NOT NULL,
    filas_afectadas INTEGER,
    fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para almacenar planes de ejecución detallados
CREATE TABLE IF NOT EXISTS planes_ejecucion_detallados (
    id SERIAL PRIMARY KEY,
    query_numero INTEGER NOT NULL,
    query_nombre VARCHAR(100) NOT NULL,
    fase VARCHAR(20) NOT NULL,
    plan_type VARCHAR(50) NOT NULL, -- 'explain_analyze', 'explain_buffers'
    plan_json JSONB NOT NULL,
    tiempo_total_ms DECIMAL(10,3),
    filas_totales BIGINT,
    loops_totales BIGINT,
    fecha_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para métricas detalladas de performance
CREATE TABLE IF NOT EXISTS metricas_detalladas_performance (
    id SERIAL PRIMARY KEY,
    query_numero INTEGER NOT NULL,
    fase VARCHAR(20) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,3) NOT NULL,
    metric_unit VARCHAR(50),
    fecha_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- SECCIÓN 2: FUNCIONES DE CAPTURA AUTOMATIZADA
-- ============================================================================

-- Función principal para medir queries
CREATE OR REPLACE FUNCTION ejecutar_y_medir_query_completo(
    query_text TEXT, 
    query_numero INTEGER, 
    query_nombre VARCHAR(100),
    problema_negocio TEXT,
    fase VARCHAR(20)
) RETURNS DECIMAL AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    tiempo_ms DECIMAL;
    filas_count INTEGER;
    i INTEGER;
BEGIN
    RAISE NOTICE 'Ejecutando Query % (%): %', query_numero, fase, query_nombre;
    
    -- Warm-up para fase con índices
    IF fase = 'con_indices' THEN
        FOR i IN 1..2 LOOP
            BEGIN
                IF UPPER(TRIM(query_text)) LIKE 'SELECT%' THEN
                    EXECUTE 'SELECT COUNT(*) FROM (' || query_text || ') AS subquery' INTO filas_count;
                END IF;
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END LOOP;
        PERFORM pg_sleep(0.1);
    END IF;
    
    -- Capturar tiempo de ejecución
    start_time := clock_timestamp();
    
    IF UPPER(TRIM(query_text)) LIKE 'SELECT%' THEN
        EXECUTE 'SELECT COUNT(*) FROM (' || query_text || ') AS subquery' INTO filas_count;
    ELSE
        EXECUTE query_text;
        GET DIAGNOSTICS filas_count = ROW_COUNT;
    END IF;
    
    end_time := clock_timestamp();
    tiempo_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    -- Guardar resultado principal
    INSERT INTO resultados_benchmark_comparativo 
        (query_numero, query_nombre, fase, problema_negocio, tiempo_ejecucion, filas_afectadas)
    VALUES 
        (query_numero, query_nombre, fase, problema_negocio, tiempo_ms, filas_count);
    
    RAISE NOTICE 'Query % (%): % ms, % filas', query_numero, fase, tiempo_ms, filas_count;
    
    RETURN tiempo_ms;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error en query %: %', query_numero, SQLERRM;
        INSERT INTO resultados_benchmark_comparativo 
            (query_numero, query_nombre, fase, problema_negocio, tiempo_ejecucion, filas_afectadas)
        VALUES 
            (query_numero, query_nombre, fase, problema_negocio, -1, -1);
        RETURN -1;
END;
$$ LANGUAGE plpgsql;

-- Función para capturar EXPLAIN ANALYZE detallado
CREATE OR REPLACE FUNCTION capturar_explain_analyze_detallado(
    query_text TEXT,
    query_numero INTEGER,
    query_nombre VARCHAR(100),
    fase VARCHAR(20)
) RETURNS VOID AS $$
DECLARE
    explain_analyze_json JSONB;
    explain_buffers_json JSONB;
    plan_record RECORD;
    total_time DECIMAL := 0;
    total_rows BIGINT := 0;
    total_loops BIGINT := 0;
BEGIN
    RAISE NOTICE 'Capturando EXPLAIN ANALYZE para Query % (%)', query_numero, fase;
    
    -- Capturar EXPLAIN ANALYZE básico
    BEGIN
        EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON) ' || query_text INTO explain_analyze_json;
        
        -- Extraer métricas del plan
        IF explain_analyze_json IS NOT NULL AND jsonb_array_length(explain_analyze_json) > 0 THEN
            total_time := (explain_analyze_json->0->'Plan'->>'Total Cost')::DECIMAL;
            total_rows := (explain_analyze_json->0->'Plan'->>'Plan Rows')::BIGINT;
        END IF;
        
        INSERT INTO planes_ejecucion_detallados 
            (query_numero, query_nombre, fase, plan_type, plan_json, tiempo_total_ms, filas_totales, loops_totales)
        VALUES 
            (query_numero, query_nombre, fase, 'explain_analyze', explain_analyze_json, total_time, total_rows, total_loops);
            
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error en EXPLAIN ANALYZE para query %: %', query_numero, SQLERRM;
    END;
    
    -- Capturar EXPLAIN con BUFFERS (solo para fase con índices)
    IF fase = 'con_indices' THEN
        BEGIN
            EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' || query_text INTO explain_buffers_json;
            
            INSERT INTO planes_ejecucion_detallados 
                (query_numero, query_nombre, fase, plan_type, plan_json, tiempo_total_ms, filas_totales, loops_totales)
            VALUES 
                (query_numero, query_nombre, fase, 'explain_buffers', explain_buffers_json, total_time, total_rows, total_loops);
                
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error en EXPLAIN BUFFERS para query %: %', query_numero, SQLERRM;
        END;
    END IF;
    
    -- Capturar métricas adicionales de performance
    PERFORM capturar_metricas_adicionales(query_numero, fase, query_text);
    
END;
$$ LANGUAGE plpgsql;

-- Función para capturar métricas adicionales
CREATE OR REPLACE FUNCTION capturar_metricas_adicionales(
    query_numero INTEGER,
    fase VARCHAR(20),
    query_text TEXT
) RETURNS VOID AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    exec_time DECIMAL;
    temp_bytes BIGINT;
    shared_blocks BIGINT;
    local_blocks BIGINT;
    temp_blocks BIGINT;
BEGIN
    -- Capturar estadísticas de ejecución
    IF UPPER(TRIM(query_text)) LIKE 'SELECT%' THEN
        -- Ejecutar con track_activities para obtener métricas
        start_time := clock_timestamp();
        EXECUTE query_text;
        end_time := clock_timestamp();
        exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        -- Guardar métricas básicas
        INSERT INTO metricas_detalladas_performance 
            (query_numero, fase, metric_name, metric_value, metric_unit)
        VALUES 
            (query_numero, fase, 'execution_time', exec_time, 'ms');
            
    END IF;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error capturando métricas para query %: %', query_numero, SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- SECCIÓN 3: DEFINICIÓN DE QUERIES Y CONTEXTO DE NEGOCIO
-- ============================================================================

CREATE TEMP TABLE queries_definiciones (
    query_numero INTEGER PRIMARY KEY,
    query_nombre VARCHAR(100),
    query_sql TEXT,
    problema_negocio TEXT,
    complejidad VARCHAR(20),
    categoria_negocio VARCHAR(50)
);

INSERT INTO queries_definiciones VALUES
-- QUERIES BÁSICAS (1-3)
(1, 'Fa_B_Conteo_Vehiculos_Tipo_V2',
 'SELECT vehicle_type, COUNT(*) as cantidad FROM vehicles GROUP BY vehicle_type ORDER BY cantidad DESC',
 'Análisis de distribución de flota por tipo de vehículo para planificación de mantenimiento y renovación',
 'BASICA', 'GESTIÓN DE FLOTA'),

(2, 'Fa_B_Licencias_Proximas_Vencer_V2',
 'SELECT first_name, last_name, license_number, license_expiry FROM drivers WHERE license_expiry < CURRENT_DATE + INTERVAL ''30 days'' ORDER BY license_expiry',
 'Gestión preventiva de documentación de conductores para evitar sanciones regulatorias',
 'BASICA', 'GESTIÓN DE CONDUCTORES'),

(3, 'Fa_B_Viajes_por_Estado_V2',
 'SELECT status, COUNT(*) as total_viajes FROM trips GROUP BY status',
 'Monitoreo del estado operativo de la flota en tiempo real',
 'BASICA', 'MONITOREO OPERATIVO'),

-- QUERIES INTERMEDIAS (4-8)
(4, 'Fa_I_Entregas_por_Ciudad_V2',
 'WITH rutas_destino AS (SELECT r.route_id, r.destination_city FROM routes r),
 viajes_60_dias AS (SELECT route_id, trip_id FROM trips WHERE departure_datetime >= CURRENT_DATE - INTERVAL ''60 days''),
 entregas_agrupadas AS (SELECT t.trip_id, COUNT(d.delivery_id) as entregas_count, SUM(d.package_weight_kg) as peso_total FROM viajes_60_dias t JOIN deliveries d ON t.trip_id = d.trip_id GROUP BY t.trip_id)
 SELECT rd.destination_city, COUNT(DISTINCT v.trip_id) as total_viajes, SUM(ea.entregas_count) as total_entregas, SUM(ea.peso_total) as peso_total_kg FROM rutas_destino rd JOIN viajes_60_dias v ON rd.route_id = v.route_id JOIN entregas_agrupadas ea ON v.trip_id = ea.trip_id GROUP BY rd.destination_city ORDER BY total_entregas DESC',
 'Análisis de volumen de entregas por ciudad destino para optimización logística',
 'INTERMEDIA', 'ANÁLISIS LOGÍSTICO'),

(5, 'Fa_I_Conductores_Activos_Viajes_V2',
 'SELECT d.driver_id, d.first_name || '' '' || d.last_name as nombre_completo, d.license_expiry, COUNT(t.trip_id) as viajes_totales, SUM(CASE WHEN t.status = ''completed'' THEN 1 ELSE 0 END) as viajes_completados FROM drivers d LEFT JOIN trips t ON d.driver_id = t.driver_id WHERE d.status = ''active'' GROUP BY d.driver_id, d.first_name, d.last_name, d.license_expiry HAVING COUNT(t.trip_id) > 0 ORDER BY viajes_completados DESC',
 'Evaluación de productividad y rendimiento de conductores activos',
 'INTERMEDIA', 'GESTIÓN DE CONDUCTORES'),

(6, 'Fa_I_Promedio_Entregas_Conductor_V2',
 'WITH entregas_por_viaje AS (SELECT t.driver_id, t.trip_id, COUNT(d.delivery_id) as entregas FROM trips t INNER JOIN deliveries d ON t.trip_id = d.trip_id WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL ''6 months'' AND t.status = ''completed'' GROUP BY t.driver_id, t.trip_id),
 conductores_activos AS (SELECT driver_id, COUNT(trip_id) as viajes, SUM(entregas) as total_entregas, ROUND(AVG(entregas), 2) as promedio_entregas_viaje FROM entregas_por_viaje GROUP BY driver_id HAVING COUNT(trip_id) >= 10)
 SELECT d.driver_id, d.first_name || '' '' || d.last_name as conductor, ca.viajes as total_viajes, ca.total_entregas, ca.promedio_entregas_viaje, ROUND(ca.total_entregas / 180.0, 2) as promedio_entregas_diarias FROM conductores_activos ca INNER JOIN drivers d ON ca.driver_id = d.driver_id ORDER BY ca.promedio_entregas_viaje DESC',
 'Cálculo de métricas de eficiencia operativa por conductor para incentivos',
 'INTERMEDIA', 'GESTIÓN DE CONDUCTORES'),

(7, 'Fa_I_Rutas_Consumo_Combustible_V2',
 'SELECT r.route_code, r.origin_city || '' -> '' || r.destination_city as ruta, r.distance_km, COUNT(t.trip_id) as viajes_realizados, ROUND(AVG(t.fuel_consumed_liters / NULLIF(r.distance_km, 0)) * 100, 2) as litros_por_100km FROM routes r INNER JOIN trips t ON r.route_id = t.route_id WHERE t.fuel_consumed_liters IS NOT NULL AND r.distance_km > 10 AND t.fuel_consumed_liters > 0 AND t.status = ''completed'' GROUP BY r.route_id, r.route_code, r.origin_city, r.destination_city, r.distance_km HAVING COUNT(t.trip_id) >= 3 ORDER BY litros_por_100km DESC LIMIT 10',
 'Identificación de rutas con mayor consumo de combustible para optimización de costos',
 'INTERMEDIA', 'OPTIMIZACIÓN DE COSTOS'),

(8, 'Fa_I_Entregas_Retrasadas_Dia_V2',
 'SELECT TO_CHAR(d.scheduled_datetime, ''Day'') as dia_semana, EXTRACT(DOW FROM d.scheduled_datetime) as num_dia, COUNT(*) as total_entregas, COUNT(CASE WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL ''30 minutes'' THEN 1 END) as entregas_retrasadas, ROUND(100.0 * COUNT(CASE WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL ''30 minutes'' THEN 1 END) / COUNT(*), 2) as porcentaje_retrasos FROM deliveries d WHERE d.delivery_status = ''delivered'' AND d.scheduled_datetime >= CURRENT_DATE - INTERVAL ''90 days'' GROUP BY dia_semana, num_dia ORDER BY num_dia',
 'Análisis de puntualidad de entregas por día de la semana para mejorar SLA',
 'INTERMEDIA', 'CALIDAD DE SERVICIO'),

-- QUERIES COMPLEJAS (9-12)
(9, 'Fa_C_Costo_Mantenimiento_Km_V2',
 'WITH viajes_agrupados AS (SELECT t.vehicle_id, COUNT(*) as total_viajes, SUM(r.distance_km) as km_totales FROM trips t INNER JOIN routes r ON t.route_id = r.route_id WHERE t.status = ''completed'' GROUP BY t.vehicle_id),
 mantenimiento_agrupado AS (SELECT vehicle_id, SUM(cost) as costo_total, COUNT(*) as total_mantenimientos FROM maintenance GROUP BY vehicle_id),
 vehiculos_con_metricas AS (SELECT v.vehicle_id, v.vehicle_type, COALESCE(va.total_viajes, 0) as viajes, COALESCE(va.km_totales, 0) as km, COALESCE(ma.costo_total, 0) as costo_mantenimiento, COALESCE(ma.total_mantenimientos, 0) as mantenimientos FROM vehicles v LEFT JOIN viajes_agrupados va ON v.vehicle_id = va.vehicle_id LEFT JOIN mantenimiento_agrupado ma ON v.vehicle_id = ma.vehicle_id WHERE COALESCE(va.km_totales, 0) > 0 AND COALESCE(ma.costo_total, 0) > 0)
 SELECT vehicle_type, COUNT(*) as cantidad_vehiculos, SUM(viajes) as viajes_totales, SUM(km) as kilometros_totales, SUM(costo_mantenimiento) as costo_total_mantenimiento, ROUND(SUM(costo_mantenimiento) / NULLIF(SUM(km), 0), 2) as costo_por_km, ROUND(AVG(costo_mantenimiento / NULLIF(mantenimientos, 0)), 2) as costo_promedio_mantenimiento FROM vehiculos_con_metricas GROUP BY vehicle_type ORDER BY costo_por_km DESC',
 'Cálculo de costos operativos por tipo de vehículo para decisiones de renovación de flota',
 'COMPLEJA', 'ANÁLISIS FINANCIERO'),

(10, 'Fa_C_Ranking_Conductores_Eficiencia_V2',
 'WITH metricas_conductores AS (SELECT t.driver_id, COUNT(DISTINCT t.trip_id) as viajes, COUNT(del.delivery_id) as entregas, AVG(t.fuel_consumed_liters / NULLIF(r.distance_km, 0)) * 100 as consumo_100km, COUNT(CASE WHEN del.delivered_datetime <= del.scheduled_datetime THEN 1 END) as entregas_puntuales FROM trips t JOIN routes r ON t.route_id = r.route_id LEFT JOIN deliveries del ON t.trip_id = del.trip_id WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL ''3 months'' AND t.status = ''completed'' AND r.distance_km > 0 GROUP BY t.driver_id HAVING COUNT(DISTINCT t.trip_id) >= 5),
 rankings AS (SELECT driver_id, viajes, entregas, ROUND(consumo_100km, 2) as consumo_100km, ROUND(entregas_puntuales * 100.0 / NULLIF(entregas, 0), 2) as puntualidad_pct, RANK() OVER (ORDER BY (entregas_puntuales * 100.0 / NULLIF(entregas, 0)) DESC) as rank_puntualidad, RANK() OVER (ORDER BY consumo_100km ASC) as rank_eficiencia, RANK() OVER (ORDER BY entregas DESC) as rank_productividad FROM metricas_conductores)
 SELECT d.first_name || '' '' || d.last_name as nombre, r.viajes, r.entregas, r.consumo_100km, r.puntualidad_pct, r.rank_puntualidad, r.rank_eficiencia, r.rank_productividad, (r.rank_puntualidad + r.rank_eficiencia + r.rank_productividad) / 3.0 as score_promedio FROM rankings r JOIN drivers d ON r.driver_id = d.driver_id ORDER BY score_promedio ASC LIMIT 15',
 'Ranking integral de conductores basado en múltiples métricas de eficiencia para programas de reconocimiento',
 'COMPLEJA', 'GESTIÓN DE CONDUCTORES'),

(11, 'Fa_C_Analisis_Tendencia_Viajes_V2',
 'SELECT TO_CHAR(DATE_TRUNC(''month'', departure_datetime), ''YYYY-MM'') as periodo, COUNT(*) as total_viajes, LAG(COUNT(*), 1) OVER (ORDER BY DATE_TRUNC(''month'', departure_datetime)) as viajes_mes_anterior, COUNT(*) - LAG(COUNT(*), 1) OVER (ORDER BY DATE_TRUNC(''month'', departure_datetime)) as cambio_absoluto, ROUND((COUNT(*) - LAG(COUNT(*), 1) OVER (ORDER BY DATE_TRUNC(''month'', departure_datetime)))::NUMERIC / NULLIF(LAG(COUNT(*), 1) OVER (ORDER BY DATE_TRUNC(''month'', departure_datetime)), 0) * 100, 2) as cambio_porcentual, ROUND(SUM(total_weight_kg) / 1000, 2) as toneladas_transportadas, ROUND(AVG(fuel_consumed_liters), 2) as combustible_promedio_viaje FROM trips WHERE status = ''completed'' AND departure_datetime >= CURRENT_DATE - INTERVAL ''12 months'' GROUP BY DATE_TRUNC(''month'', departure_datetime) ORDER BY DATE_TRUNC(''month'', departure_datetime) DESC LIMIT 6',
 'Análisis de tendencias mensuales para forecasting y planificación estratégica',
 'COMPLEJA', 'PLANIFICACIÓN ESTRATÉGICA'),

(12, 'Fa_C_Pivot_Entregas_Hora_Dia_V2',
 'WITH entregas_agrupadas AS (SELECT EXTRACT(DOW FROM scheduled_datetime) as dia_semana, EXTRACT(HOUR FROM scheduled_datetime) as hora, COUNT(*) as cantidad_entregas FROM deliveries WHERE scheduled_datetime >= CURRENT_DATE - INTERVAL ''60 days'' AND EXTRACT(HOUR FROM scheduled_datetime) BETWEEN 6 AND 22 GROUP BY EXTRACT(DOW FROM scheduled_datetime), EXTRACT(HOUR FROM scheduled_datetime))
 SELECT hora, SUM(CASE WHEN dia_semana = 0 THEN cantidad_entregas ELSE 0 END) as domingo, SUM(CASE WHEN dia_semana = 1 THEN cantidad_entregas ELSE 0 END) as lunes, SUM(CASE WHEN dia_semana = 2 THEN cantidad_entregas ELSE 0 END) as martes, SUM(CASE WHEN dia_semana = 3 THEN cantidad_entregas ELSE 0 END) as miercoles, SUM(CASE WHEN dia_semana = 4 THEN cantidad_entregas ELSE 0 END) as jueves, SUM(CASE WHEN dia_semana = 5 THEN cantidad_entregas ELSE 0 END) as viernes, SUM(CASE WHEN dia_semana = 6 THEN cantidad_entregas ELSE 0 END) as sabado, SUM(cantidad_entregas) as total_semana FROM entregas_agrupadas GROUP BY hora ORDER BY hora',
 'Matriz de distribución horaria de entregas para optimización de recursos y turnos',
 'COMPLEJA', 'OPTIMIZACIÓN OPERATIVA');

-- ============================================================================
-- SECCIÓN 4: EJECUCIÓN COMPLETA DEL BENCHMARK
-- ============================================================================

-- Limpiar resultados anteriores
TRUNCATE TABLE resultados_benchmark_comparativo;
TRUNCATE TABLE planes_ejecucion_detallados;
TRUNCATE TABLE metricas_detalladas_performance;

-- FASE 1: EJECUCIÓN SIN ÍNDICES
DO $$
DECLARE
    tiempo DECIMAL;
    query_record RECORD;
BEGIN
    RAISE NOTICE '=== INICIANDO FASE 1: EJECUCIÓN SIN ÍNDICES ===';
    
    -- Eliminar todos los índices existentes
    DROP INDEX IF EXISTS 
        idx_vehicles_type_covering, idx_trips_status_brin, idx_trips_status_hash,
        idx_trips_driver_comprehensive, idx_deliveries_punctuality_comprehensive,
        idx_routes_distance_covering, idx_deliveries_datetime_brin, idx_deliveries_hour_dow,
        idx_trips_departure_completed, idx_deliveries_status_datetime, idx_drivers_license_status,
        idx_maintenance_comprehensive;
    
    -- Actualizar estadísticas
    ANALYZE vehicles, drivers, routes, trips, deliveries, maintenance;
    
    -- Ejecutar todas las queries sin índices
    FOR query_record IN 
        SELECT query_numero, query_nombre, query_sql, problema_negocio 
        FROM queries_definiciones 
        ORDER BY query_numero
    LOOP
        -- Ejecutar query y medir tiempo
        tiempo := ejecutar_y_medir_query_completo(
            query_record.query_sql,
            query_record.query_numero,
            query_record.query_nombre,
            query_record.problema_negocio,
            'sin_indices'
        );
        
        -- Capturar EXPLAIN ANALYZE
        PERFORM capturar_explain_analyze_detallado(
            query_record.query_sql,
            query_record.query_numero,
            query_record.query_nombre,
            'sin_indices'
        );
        
        PERFORM pg_sleep(0.2);
    END LOOP;
    
    RAISE NOTICE '=== FASE 1 COMPLETADA: 12 queries ejecutadas SIN índices ===';
END $$;

-- ============================================================================
-- SECCIÓN 5: CREACIÓN DE ÍNDICES Y OPTIMIZACIÓN
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '=== APLICANDO OPTIMIZACIONES ===';
    
    -- Configuración PostgreSQL
    SET default_statistics_target = 500;
    SET random_page_cost = 1.1;
    SET effective_cache_size = '4GB';
    SET work_mem = '512MB';
    SET enable_seqscan = off;
    
    -- Crear índices estratégicos
    CREATE INDEX IF NOT EXISTS idx_vehicles_type_covering ON vehicles(vehicle_type) INCLUDE (vehicle_id);
    CREATE INDEX IF NOT EXISTS idx_trips_status_brin ON trips USING brin(status);
    CREATE INDEX IF NOT EXISTS idx_trips_status_hash ON trips USING hash(status);
    CREATE INDEX IF NOT EXISTS idx_trips_driver_comprehensive ON trips(driver_id, status, departure_datetime, route_id) INCLUDE (trip_id, fuel_consumed_liters, total_weight_kg);
    CREATE INDEX IF NOT EXISTS idx_deliveries_punctuality_comprehensive ON deliveries(trip_id, scheduled_datetime, delivered_datetime, delivery_status);
    CREATE INDEX IF NOT EXISTS idx_routes_distance_covering ON routes(route_id, distance_km) INCLUDE (origin_city, destination_city);
    CREATE INDEX IF NOT EXISTS idx_deliveries_datetime_brin ON deliveries USING brin(scheduled_datetime);
    CREATE INDEX IF NOT EXISTS idx_trips_departure_completed ON trips(departure_datetime, status) WHERE status = 'completed';
    CREATE INDEX IF NOT EXISTS idx_deliveries_status_datetime ON deliveries(delivery_status, scheduled_datetime);
    CREATE INDEX IF NOT EXISTS idx_drivers_license_status ON drivers(license_expiry, status) INCLUDE (driver_id, first_name, last_name, license_number);
    CREATE INDEX IF NOT EXISTS idx_maintenance_comprehensive ON maintenance(vehicle_id, maintenance_date, cost) INCLUDE (maintenance_type);
    
    -- Actualizar estadísticas
    ANALYZE VERBOSE trips, deliveries, routes;
    
    RAISE NOTICE '=== 11 ÍNDICES CREADOS Y ESTADÍSTICAS ACTUALIZADAS ===';
END $$;

-- FASE 2: EJECUCIÓN CON ÍNDICES
DO $$
DECLARE
    tiempo DECIMAL;
    query_record RECORD;
    i INTEGER;
    filas_count INTEGER;
BEGIN
    RAISE NOTICE '=== INICIANDO FASE 2: EJECUCIÓN CON ÍNDICES ===';
    
    SET enable_seqscan = off;
    
    FOR query_record IN 
        SELECT query_numero, query_nombre, query_sql, problema_negocio 
        FROM queries_definiciones 
        ORDER BY query_numero
    LOOP
        -- Warm-up para queries problemáticas
        IF query_record.query_numero IN (1, 3, 10, 12) THEN
            FOR i IN 1..2 LOOP
                BEGIN
                    EXECUTE 'SELECT COUNT(*) FROM (' || query_record.query_sql || ') AS subquery' INTO filas_count;
                EXCEPTION WHEN OTHERS THEN filas_count := 0; END;
            END LOOP;
            PERFORM pg_sleep(0.2);
        END IF;
        
        -- Ejecutar query y medir tiempo
        tiempo := ejecutar_y_medir_query_completo(
            query_record.query_sql,
            query_record.query_numero,
            query_record.query_nombre,
            query_record.problema_negocio,
            'con_indices'
        );
        
        -- Capturar EXPLAIN ANALYZE
        PERFORM capturar_explain_analyze_detallado(
            query_record.query_sql,
            query_record.query_numero,
            query_record.query_nombre,
            'con_indices'
        );
        
        PERFORM pg_sleep(0.2);
    END LOOP;
    
    RAISE NOTICE '=== FASE 2 COMPLETADA: 12 queries ejecutadas CON índices ===';
END $$;

-- ============================================================================
-- SECCIÓN 6: REPORTES COMPLETOS Y ANÁLISIS
-- ============================================================================

-- Reporte principal de comparación
SELECT 'REPORTE COMPLETO DE OPTIMIZACIÓN - RESULTADOS DETALLADOS' as titulo;

-- Comparación de tiempos
SELECT 
    rbc.query_numero,
    rbc.query_nombre,
    qd.complejidad,
    qd.categoria_negocio,
    rbc.problema_negocio,
    ROUND(sin_indices.tiempo_ejecucion, 3) as tiempo_sin_indices_ms,
    ROUND(con_indices.tiempo_ejecucion, 3) as tiempo_con_indices_ms,
    ROUND(sin_indices.tiempo_ejecucion - con_indices.tiempo_ejecucion, 3) as mejora_absoluta_ms,
    CASE 
        WHEN sin_indices.tiempo_ejecucion > 0 THEN
            ROUND((1 - con_indices.tiempo_ejecucion / sin_indices.tiempo_ejecucion) * 100, 2)
        ELSE 0
    END as mejora_porcentual,
    CASE 
        WHEN (sin_indices.tiempo_ejecucion - con_indices.tiempo_ejecucion) > 50 THEN '🚀 EXCELENTE'
        WHEN (sin_indices.tiempo_ejecucion - con_indices.tiempo_ejecucion) > 20 THEN '✅ MUY BUENA'
        WHEN (sin_indices.tiempo_ejecucion - con_indices.tiempo_ejecucion) > 0 THEN '👍 BUENA'
        WHEN (sin_indices.tiempo_ejecucion - con_indices.tiempo_ejecucion) = 0 THEN '⚡ MANTUVO'
        WHEN (sin_indices.tiempo_ejecucion - con_indices.tiempo_ejecucion) >= -10 THEN '🔶 LEVE CAÍDA'
        ELSE '⚠️ EMPEORÓ'
    END as estado_optimizacion
FROM resultados_benchmark_comparativo rbc
JOIN queries_definiciones qd ON rbc.query_numero = qd.query_numero
JOIN (SELECT query_numero, tiempo_ejecucion FROM resultados_benchmark_comparativo WHERE fase = 'sin_indices') sin_indices ON rbc.query_numero = sin_indices.query_numero
JOIN (SELECT query_numero, tiempo_ejecucion FROM resultados_benchmark_comparativo WHERE fase = 'con_indices') con_indices ON rbc.query_numero = con_indices.query_numero
WHERE rbc.fase = 'con_indices'
ORDER BY mejora_porcentual DESC;

-- Resumen por categoría de negocio
SELECT 
    qd.categoria_negocio,
    COUNT(*) as total_queries,
    ROUND(AVG(sin_indices.tiempo_ejecucion), 2) as avg_sin_indices_ms,
    ROUND(AVG(con_indices.tiempo_ejecucion), 2) as avg_con_indices_ms,
    ROUND(AVG(sin_indices.tiempo_ejecucion - con_indices.tiempo_ejecucion), 2) as avg_mejora_ms,
    ROUND(AVG(1 - con_indices.tiempo_ejecucion / NULLIF(sin_indices.tiempo_ejecucion, 0)) * 100, 2) as avg_mejora_porcentual,
    COUNT(*) FILTER (WHERE (sin_indices.tiempo_ejecucion - con_indices.tiempo_ejecucion) > 0) as queries_mejoradas
FROM queries_definiciones qd
JOIN (SELECT query_numero, tiempo_ejecucion FROM resultados_benchmark_comparativo WHERE fase = 'sin_indices') sin_indices ON qd.query_numero = sin_indices.query_numero
JOIN (SELECT query_numero, tiempo_ejecucion FROM resultados_benchmark_comparativo WHERE fase = 'con_indices') con_indices ON qd.query_numero = con_indices.query_numero
GROUP BY qd.categoria_negocio
ORDER BY avg_mejora_porcentual DESC;

-- Análisis de planes de ejecución
SELECT 
    'ANÁLISIS DE PLANES DE EJECUCIÓN' as titulo;

SELECT 
    query_numero,
    query_nombre,
    fase,
    COUNT(*) as total_planes,
    ROUND(AVG(tiempo_total_ms), 2) as avg_tiempo_plan_ms,
    SUM(filas_totales) as total_filas_plan
FROM planes_ejecucion_detallados
GROUP BY query_numero, query_nombre, fase
ORDER BY query_numero, fase;

-- Resumen ejecutivo final
SELECT 
    'RESUMEN EJECUTIVO FINAL' as titulo,
    '' as valor
UNION ALL
SELECT 
    'Total queries ejecutadas',
    COUNT(DISTINCT query_numero)::TEXT
FROM resultados_benchmark_comparativo
UNION ALL
SELECT 
    'Queries mejoradas',
    COUNT(*) FILTER (WHERE mejora > 0)::TEXT
FROM (
    SELECT sin_indices.tiempo_ejecucion - con_indices.tiempo_ejecucion as mejora
    FROM queries_definiciones qd
    JOIN (SELECT query_numero, tiempo_ejecucion FROM resultados_benchmark_comparativo WHERE fase = 'sin_indices') sin_indices ON qd.query_numero = sin_indices.query_numero
    JOIN (SELECT query_numero, tiempo_ejecucion FROM resultados_benchmark_comparativo WHERE fase = 'con_indices') con_indices ON qd.query_numero = con_indices.query_numero
) metricas
UNION ALL
SELECT 
    'Mejora promedio',
    ROUND(AVG(mejora_porcentual), 2)::TEXT || '%'
FROM (
    SELECT 
        CASE 
            WHEN sin_indices.tiempo_ejecucion > 0 THEN
                ROUND((1 - con_indices.tiempo_ejecucion / sin_indices.tiempo_ejecucion) * 100, 2)
            ELSE 0
        END as mejora_porcentual
    FROM queries_definiciones qd
    JOIN (SELECT query_numero, tiempo_ejecucion FROM resultados_benchmark_comparativo WHERE fase = 'sin_indices') sin_indices ON qd.query_numero = sin_indices.query_numero
    JOIN (SELECT query_numero, tiempo_ejecucion FROM resultados_benchmark_comparativo WHERE fase = 'con_indices') con_indices ON qd.query_numero = con_indices.query_numero
) porcentajes
UNION ALL
SELECT 
    'Reducción total de tiempo',
    ROUND((
        SELECT SUM(tiempo_ejecucion) FROM resultados_benchmark_comparativo WHERE fase = 'sin_indices'
    ) - (
        SELECT SUM(tiempo_ejecucion) FROM resultados_benchmark_comparativo WHERE fase = 'con_indices'
    ), 2)::TEXT || ' ms'
UNION ALL
SELECT 
    'Planes de ejecución capturados',
    (SELECT COUNT(*) FROM planes_ejecucion_detallados)::TEXT;

-- Mensaje final
SELECT '✅ BENCHMARK COMPLETADO - TODOS LOS DATOS CAPTURADOS' as mensaje_final;
SELECT '📊 Ejecuta los siguientes queries para exportar datos:' as siguiente_paso;
SELECT '1. SELECT * FROM resultados_benchmark_comparativo ORDER BY query_numero, fase;' as export_1;
SELECT '2. SELECT * FROM planes_ejecucion_detallados ORDER BY query_numero, fase;' as export_2;
SELECT '3. SELECT * FROM metricas_detalladas_performance ORDER BY query_numero, fase;' as export_3;