import argparse
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.ml.metrics import calcular_metricas_regresion
from src.ml.pipeline import crear_pipeline
from src.spark_session import crear_spark
from src.utils.fechas import resolver_meses
from src.utils.logging import configurar_logging

def unir_ruta(uri, path):
    if uri:
        return uri.rstrip("/") + path
    return path

def main():
    parser = argparse.ArgumentParser(description="Entrenamiento Spark ML TLC")
    parser.add_argument("--year", required=True, type=int, help="Anio")
    parser.add_argument("--month", help="Mes (01-12)")
    parser.add_argument("--months", help="Lista de meses separados por coma")
    parser.add_argument("--quarter", type=int, help="Trimestre (1-4)")
    parser.add_argument("--hdfs-uri", default="hdfs://namenode:8020", help="URI HDFS")
    parser.add_argument("--curated-root", default="/data/tlc/curated", help="Ruta curated")
    parser.add_argument("--model-root", default="/models/tlc_trip_duration", help="Ruta modelo")
    parser.add_argument(
        "--metrics-root",
        default="/reports/metrics/tlc_trip_duration",
        help="Ruta metrics",
    )
    parser.add_argument("--master", default=None, help="Master de Spark")
    parser.add_argument("--app-name", default="tlc-train", help="Nombre de la app")
    parser.add_argument(
        "--algoritmo",
        default="gbt",
        choices=["gbt", "rf"],
        help="Algoritmo de regresion",
    )
    parser.add_argument(
        "--sample-frac",
        type=float,
        default=None,
        help="Fraccion de muestreo para entrenamiento",
    )
    parser.add_argument(
        "--max-rows", type=int, default=None, help="Limite de filas para entrenamiento"
    )
    args = parser.parse_args()

    logger = configurar_logging("train_tlc")
    spark = crear_spark(app_name=args.app_name, master=args.master)

    try:
        meses = resolver_meses(args.month, args.months, args.quarter)
    except ValueError as error:
        logger.error(str(error))
        sys.exit(1)

    for mes in meses:
        ruta_entrada = unir_ruta(
            args.hdfs_uri, f"{args.curated_root}/year={args.year}/month={mes}"
        )
        logger.info("Leyendo datos curated desde %s", ruta_entrada)
        df = spark.read.parquet(ruta_entrada)

        df = df.filter(F.col("trip_duration_min") > 0)
        if args.sample_frac and 0 < args.sample_frac < 1:
            df = df.sample(fraction=args.sample_frac, seed=42)
        if args.max_rows and args.max_rows > 0:
            df = df.limit(args.max_rows)

        columnas_numericas_base = [
            "trip_distance",
            "fare_amount",
            "passenger_count",
            "pickup_hour",
            "pickup_dow",
            "is_weekend",
        ]
        columnas_categoricas_base = [
            "payment_type",
            "pu_location",
            "do_location",
            "vendor_id",
            "ratecode_id",
        ]

        columnas_numericas = [c for c in columnas_numericas_base if c in df.columns]
        columnas_categoricas = [c for c in columnas_categoricas_base if c in df.columns]

        if not columnas_numericas:
            raise ValueError("No hay columnas numericas disponibles para el modelo")

        pipeline = crear_pipeline(
            columnas_numericas=columnas_numericas,
            columnas_categoricas=columnas_categoricas,
            algoritmo=args.algoritmo,
            col_label="trip_duration_min",
        )

        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        rows_train = train_df.count()
        rows_test = test_df.count()
        logger.info("Entrenando modelo con %s filas", rows_train)

        modelo = pipeline.fit(train_df)
        pred = modelo.transform(test_df)

        metricas = calcular_metricas_regresion(pred)
        logger.info("Metricas: %s", metricas)

        ruta_modelo = unir_ruta(
            args.hdfs_uri, f"{args.model_root}/year={args.year}/month={mes}"
        )
        logger.info("Guardando modelo en %s", ruta_modelo)
        modelo.write().overwrite().save(ruta_modelo)

        fecha_entrenamiento = datetime.now(timezone.utc).isoformat()
        resumen = {
            "year": str(args.year),
            "month": str(mes),
            "algoritmo": args.algoritmo,
            "rmse": metricas["rmse"],
            "mae": metricas["mae"],
            "r2": metricas["r2"],
            "rows_train": rows_train,
            "rows_test": rows_test,
            "fecha_entrenamiento": fecha_entrenamiento,
        }

        ruta_metricas = unir_ruta(
            args.hdfs_uri, f"{args.metrics_root}/year={args.year}/month={mes}"
        )
        logger.info("Guardando metricas en %s", ruta_metricas)

        spark.createDataFrame([resumen]).coalesce(1).write.mode("overwrite").json(
            ruta_metricas
        )

    spark.stop()

if __name__ == "__main__":
    main()
