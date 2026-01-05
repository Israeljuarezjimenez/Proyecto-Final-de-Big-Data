# AGENTS

Reglas principales para este proyecto:
- Todo el codigo y los comentarios deben estar en espanol.
- Evitar analisis narrativo largo dentro del codigo.
- Todos los scripts deben ejecutarse por CLI (argparse o flags).
- No usar rutas hardcodeadas; usar parametros y defaults claros.
- El proyecto debe ser reproducible con Docker.

Stack tecnologico:
- Python (PySpark)
- HDFS para almacenamiento
- Spark para ETL, agregaciones y ML
- Spark MLlib para el modelo
- Parquet como formato de datos
- Streamlit para visualizacion final

Estructura HDFS obligatoria:
- /data/tlc/raw/year=YYYY/month=MM
- /data/tlc/curated/year=YYYY/month=MM
- /data/tlc/marts/year=YYYY/month=MM
- /models/tlc_trip_duration/
- /reports/metrics/tlc_trip_duration/

Modelo:
- Tipo: regresion
- Target: trip_duration_min
- Algoritmo: GBTRegressor o RandomForestRegressor
- Metricas obligatorias: RMSE, MAE, R2

Calidad del codigo:
- Manejar cambios de esquema del dataset TLC
- Validar columnas antes de usarlas
- Logs claros con print o logging
- Manejo basico de errores

Entregables:
- docker/docker-compose.yml (HDFS + Spark)
- scripts/00_download_tlc.py a 06_export_for_dashboard.py
- src/ con modulos reutilizables
- dashboards/streamlit_app.py
- README.md con instrucciones reproducibles
- .gitignore
- Smoke test con un mes
