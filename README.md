# bigdata-pipeline-nyc-tlc

Pipeline de Big Data para NYC TLC (yellow taxi) con ingesta, procesamiento distribuido, modelo predictivo y dashboard.

## Arquitectura (texto)
- Contenedores Docker: HDFS (namenode + datanode) y Spark (master + worker).
- Ingesta local de Parquet a `data/raw/` y carga a HDFS en `/data/tlc/raw`.
- ETL en Spark: limpieza, features y datos curated en `/data/tlc/curated`.
- EDA y agregaciones en Spark: marts en `/data/tlc/marts` (top zonas, pagos, distancia, variabilidad).
- Modelo Spark MLlib: regresion de `trip_duration_min` y metricas en `/reports/metrics`.
- Batch scoring en Spark: predicciones en `/data/tlc/predictions`.
- Export local para Streamlit: `data/export/`.

## Dataset
- Fuente: NYC TLC Yellow Taxi.
- Formato: Parquet.
- Descarga parametrizada por anio y mes (o lista de meses para un anio completo).

## Requisitos
- Docker y Docker Compose.
- Streamlit en el host (para la vista local).

## Ejecucion paso a paso

1) Construir imagenes y levantar infraestructura
```bash
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up -d
```

2) Descargar datos (anio completo, ejemplo 2024)
```bash
docker exec -i spark-master bash -lc "cd /opt/project && python3 scripts/00_download_tlc.py --year 2024 --months 01,02,03,04,05,06,07,08,09,10,11,12 --skip-missing"
```
Si quieres un solo mes para pruebas:
```bash
docker exec -i spark-master bash -lc "cd /opt/project && python3 scripts/00_download_tlc.py --year 2024 --month 01"
```

3) Subir a HDFS
```bash
docker exec -i namenode bash -lc "cd /opt/project && scripts/01_put_to_hdfs.sh --year 2024 --months 01,02,03,04,05,06,07,08,09,10,11,12 --skip-missing"
```
Si quieres un solo mes para pruebas:
```bash
docker exec -i namenode bash -lc "cd /opt/project && scripts/01_put_to_hdfs.sh --year 2024 --month 01"
```

4) ETL con Spark
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/02_spark_etl.py --year 2024 --months 01,02,03,04,05,06,07,08,09,10,11,12"
```
Si quieres un solo mes para pruebas:
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/02_spark_etl.py --year 2024 --month 01"
```

5) EDA y agregaciones
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/03_spark_eda_agg.py --year 2024 --months 01,02,03,04,05,06,07,08,09,10,11,12"
```
Si quieres un solo mes para pruebas:
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/03_spark_eda_agg.py --year 2024 --month 01"
```

6) Entrenar modelo
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/04_train_sparkml.py --year 2024 --months 01,02,03,04,05,06,07,08,09,10,11,12 --algoritmo gbt"
```
Si quieres un solo mes para pruebas:
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/04_train_sparkml.py --year 2024 --month 01 --algoritmo gbt"
```

7) Batch scoring
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/05_batch_scoring.py --year 2024 --months 01,02,03,04,05,06,07,08,09,10,11,12"
```
Si quieres un solo mes para pruebas:
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/05_batch_scoring.py --year 2024 --month 01"
```

8) Exportar para dashboard
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/06_export_for_dashboard.py --year 2024 --months 01,02,03,04,05,06,07,08,09,10,11,12 --usar-subdir --exportar-metricas --exportar-errores"
```
Si quieres un solo mes para pruebas:
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master spark://spark-master:7077 scripts/06_export_for_dashboard.py --year 2024 --month 01"
```

9) Dashboard Streamlit (en el host)
```bash
streamlit run dashboards/streamlit_app.py
```
Si exportaste varios meses, usa el selector de periodo (incluye resumen anual).

## Puertos utiles
- HDFS UI: http://localhost:9870
- Spark Master UI: http://localhost:18080
- Spark Worker UI: http://localhost:18081

## Salidas esperadas
- HDFS raw: `/data/tlc/raw/year=YYYY/month=MM`
- HDFS curated: `/data/tlc/curated/year=YYYY/month=MM`
- HDFS marts: `/data/tlc/marts/year=YYYY/month=MM`
- HDFS models: `/models/tlc_trip_duration/`
- HDFS metrics: `/reports/metrics/tlc_trip_duration/`
- Local export: `data/export/`
  - Subdirectorios: `data/export/year=YYYY/month=MM/`
  - Incluye: `kpis`, `viajes_por_hora_dia`, `duracion_promedio_hora`, `tarifa_promedio_hora`,
    `top_origen`, `top_destino`, `pagos`, `vendor`, `distancia_bins`, `variabilidad_hora`,
    `variabilidad_dia`, `metricas_modelo`, `errores_por_hora`.

## Smoke test
Usa un solo mes (ej. 2024-01) para pruebas rapidas y verifica que existan:
- raw, curated, marts, modelo, metricas y export.
Si necesitas reducir costo en una prueba rapida:
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master local[*] scripts/02_spark_etl.py --year 2024 --month 01 --sample-frac 0.05 --max-rows 10000 --sin-outliers"
```
Para entrenamiento y scoring ligeros:
```bash
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master local[*] scripts/04_train_sparkml.py --year 2024 --month 01 --max-rows 10000"
docker exec -i spark-master bash -lc "cd /opt/project && /spark/bin/spark-submit --master local[*] scripts/05_batch_scoring.py --year 2024 --month 01 --max-rows 10000"
```

## Notas
- Si ejecutas scripts fuera del contenedor, exporta `PYTHONPATH=.`
- El dashboard lee `data/export`; puedes cambiar la ruta con `TLC_EXPORT_DIR`.
- El dashboard incluye tendencias mensuales, mapa de calor, comparativos semana/fin de semana,
  top zonas, distribucion por pagos/vendor, variabilidad y errores por hora.
