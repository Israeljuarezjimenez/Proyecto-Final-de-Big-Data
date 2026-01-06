# Informe tecnico - Pipeline Big Data NYC TLC

## 1. Problema y motivacion
El objetivo es construir un pipeline de Big Data reproducible que permita ingerir, procesar, analizar y modelar datos de movilidad urbana (NYC TLC Yellow Taxi), entregando resultados en un dashboard interactivo. La motivacion es contar con un flujo completo que soporte analisis operativos y un modelo predictivo de duracion de viajes.

## 2. Arquitectura del sistema (texto)
Arquitectura basada en contenedores Docker con HDFS y Spark. El flujo de datos parte de Parquet locales, se carga a HDFS, se transforma con Spark y se exporta para visualizacion.

```
          +-------------------+
          |  NYC TLC Parquet  |
          +---------+---------+
                    |
                    v
          +-------------------+          +----------------------+
          | data/raw +        |          | HDFS /data/tlc/raw   |
          | manifest.json     +--------->+ year=YYYY/month=MM   |
          +-------------------+          +----------+-----------+
                                                     |
                                                     v
                                       +---------------------------+
                                       | Spark ETL (curated)       |
                                       | /data/tlc/curated         |
                                       +-------------+-------------+
                                                     |
                                                     v
                                       +---------------------------+
                                       | Spark EDA (marts)         |
                                       | /data/tlc/marts           |
                                       +-------------+-------------+
                                                     |
                                                     v
                                       +---------------------------+
                                       | Spark ML (modelo)         |
                                       | /models y /reports        |
                                       +-------------+-------------+
                                                     |
                                                     v
                                       +---------------------------+
                                       | Batch scoring             |
                                       | /data/tlc/predictions     |
                                       +-------------+-------------+
                                                     |
                                                     v
                                       +---------------------------+
                                       | Export CSV local          |
                                       | data/export/              |
                                       +-------------+-------------+
                                                     |
                                                     v
                                       +---------------------------+
                                       | Streamlit Dashboard        |
                                       +---------------------------+
```

## 3. Dataset y caracteristicas del volumen
**Fuente oficial:** NYC TLC Trip Record Data (Yellow Taxi)  
Referencia: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

**Cobertura usada:** 2024 completo (12 archivos mensuales).  
**Formato:** Parquet columnar, un archivo por mes (`yellow_tripdata_2024-MM.parquet`).  
**Volumen en disco (raw local):** 660.90 MB.  
- Min: 47.65 MB  
- Mediana: 57.30 MB  
- Max: 61.37 MB  

**Esquema observado (union de columnas):** 19 columnas principales, incluyendo:
`tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`, `passenger_count`,
`fare_amount`, `total_amount`, `tip_amount`, `tolls_amount`, `extra`, `mta_tax`,
`improvement_surcharge`, `congestion_surcharge`, `Airport_fee`, `VendorID`,
`RatecodeID`, `PULocationID`, `DOLocationID`, `payment_type`, `store_and_fwd_flag`.

**Calidad y cambios de esquema:**  
El pipeline estandariza columnas y agrega defaults para tolerar cambios. En el manifest se detecto un mes con columnas vacias (2024-10), por lo que el ETL valida columnas requeridas antes de procesar.

**Registros por mes (curated, despues de limpieza):**

| Mes | Registros |
|-----|-----------|
| 01  | 2,869,602 |
| 02  | 2,901,430 |
| 03  | 3,439,876 |
| 04  | 3,414,080 |
| 05  | 3,616,288 |
| 06  | 3,428,072 |
| 07  | 2,973,807 |
| 08  | 2,867,241 |
| 09  | 3,483,694 |
| 10  | 3,681,713 |
| 11  | 3,509,849 |
| 12  | 3,518,643 |

**Total general (curated 2024):** 39,704,295 registros.

## 4. Procesamiento realizado (Spark)
1) **Ingesta y carga a HDFS**  
- `scripts/00_download_tlc.py` descarga por `--year` y `--month/--months/--quarter`, genera `manifest.json`.  
- `scripts/01_put_to_hdfs.sh` crea `/data/tlc/raw/year=YYYY/month=MM` y sube Parquet a HDFS.

2) **ETL en Spark**  
Implementado en `scripts/02_spark_etl.py` y `src/etl/`.  
- Estandariza nombres de columnas y valida requeridas.  
- Agrega columnas faltantes con defaults para soportar cambios de esquema.  
- Filtra registros invalidos: distancia <= 0, tarifa <= 0, timestamps nulos.  
- Calcula `trip_duration_min` y features temporales (`pickup_hour`, `pickup_dow`, `is_weekend`).  
- Filtra outliers con percentiles p1–p99 en distancia, tarifa y duracion.  
- Salida en Parquet particionado: `/data/tlc/curated/year=YYYY/month=MM`.

3) **EDA y marts en Spark**  
Implementado en `scripts/03_spark_eda_agg.py`.  
Genera tablas analiticas en `/data/tlc/marts/year=YYYY/month=MM`:
- `viajes_por_hora_dia`, `duracion_promedio_hora`, `tarifa_promedio_hora`  
- `kpis` (total_viajes, duracion_promedio_min, tarifa_promedio)  
- `top_origen`, `top_destino` (top zonas)  
- `pagos`, `vendor` (distribuciones)  
- `distancia_bins` (segmentacion de distancia)  
- `variabilidad_hora`, `variabilidad_dia` (p25, p50, p75 y desviacion)

4) **Batch scoring**  
`scripts/05_batch_scoring.py` aplica el modelo sobre curated y guarda en  
`/data/tlc/predictions/year=YYYY/month=MM`.

5) **Export para dashboard**  
`scripts/06_export_for_dashboard.py` exporta marts, metricas y errores a  
`data/export/year=YYYY/month=MM/` en CSV para Streamlit.

## 5. Modelo predictivo y metricas (Spark ML)
**Target:** `trip_duration_min` (duracion en minutos).  
**Tipo:** regresion.

**Features:**
- Numericas: `trip_distance`, `fare_amount`, `passenger_count`, `pickup_hour`, `pickup_dow`, `is_weekend`.  
- Categoricas: `payment_type`, `pu_location`, `do_location`, `vendor_id`, `ratecode_id`.

**Pipeline ML (src/ml/pipeline.py):**
- `StringIndexer` para categoricas.  
- `OneHotEncoder` para one-hot.  
- `VectorAssembler` para crear `features`.  
- Algoritmo configurable: `GBTRegressor` (default) o `RandomForestRegressor`.

**Entrenamiento y evaluacion:**
- `scripts/04_train_sparkml.py` con split 80/20 (seed 42).  
- Metricas obligatorias: **RMSE, MAE, R2** (guardadas en JSON).  
- Modelo en `/models/tlc_trip_duration/year=YYYY/month=MM`.  
- Metricas en `/reports/metrics/tlc_trip_duration/year=YYYY/month=MM`.

**Error por hora (post-modelo):**
- `scripts/06_export_for_dashboard.py` calcula MAE/RMSE por `pickup_hour` usando predicciones.

## 6. Resultados y visualizacion
Dashboard en Streamlit (`dashboards/streamlit_app.py`) con:
- KPIs globales y tendencias mensuales.  
- Series por hora y dia, comparativo semana vs fin de semana.  
- Mapa de calor hora x dia.  
- Top zonas de origen/destino, distribucion por pagos y vendor.  
- Distancia vs duracion/tarifa, variabilidad por hora y dia.  
- Resumen del modelo (RMSE, MAE, R2) y error por hora.

## 7. Conclusiones y trabajo futuro
- El pipeline procesa 12 meses de TLC 2024, con limpieza, features y marts analiticos.  
- Se entreno un modelo de regresion con Spark ML y se evaluo con RMSE/MAE/R2.  
- El dashboard permite explorar demanda, tarifas, patrones temporales y error del modelo.  

**Trabajo futuro:**
- Ajuste de hiperparametros y validacion cruzada.  
- Incorporar variables externas (clima, eventos, zonas).  
- Modelos por zona o por franja horaria.  
- Monitoreo continuo y drift de distribucion.
