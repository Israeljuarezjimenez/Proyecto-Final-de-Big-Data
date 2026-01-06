import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.etl.clean import (
    asegurar_columnas,
    estandarizar_columnas,
    filtrar_invalidos,
    filtrar_outliers,
    validar_columnas,
)
from src.etl.features import (
    agregar_duracion,
    agregar_features_temporales,
    agregar_particiones,
)
from src.spark_session import crear_spark
from src.utils.fechas import resolver_meses
from src.utils.logging import configurar_logging

def unir_ruta(uri, path):
    if uri:
        return uri.rstrip("/") + path
    return path

def main():
    parser = argparse.ArgumentParser(description="ETL con Spark para TLC")
    parser.add_argument("--year", required=True, type=int, help="Anio")
    parser.add_argument("--month", help="Mes (01-12)")
    parser.add_argument("--months", help="Lista de meses separados por coma")
    parser.add_argument("--quarter", type=int, help="Trimestre (1-4)")
    parser.add_argument("--hdfs-uri", default="hdfs://namenode:8020", help="URI HDFS")
    parser.add_argument("--raw-root", default="/data/tlc/raw", help="Ruta base raw")
    parser.add_argument(
        "--curated-root", default="/data/tlc/curated", help="Ruta base curated"
    )
    parser.add_argument("--master", default=None, help="Master de Spark")
    parser.add_argument("--app-name", default="tlc-etl", help="Nombre de la app")
    parser.add_argument("--p1", type=float, default=0.01, help="Percentil inferior")
    parser.add_argument("--p99", type=float, default=0.99, help="Percentil superior")
    parser.add_argument(
        "--sample-frac",
        type=float,
        default=None,
        help="Fraccion de muestreo para pruebas",
    )
    parser.add_argument(
        "--max-rows", type=int, default=None, help="Limite de filas para pruebas"
    )
    parser.add_argument(
        "--sin-outliers", action="store_true", help="Omitir filtro de outliers"
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Omitir meses que no existan en HDFS",
    )
    args = parser.parse_args()

    logger = configurar_logging("etl_tlc")
    spark = crear_spark(app_name=args.app_name, master=args.master)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        meses = resolver_meses(args.month, args.months, args.quarter)
    except ValueError as error:
        logger.error(str(error))
        sys.exit(1)

    ruta_salida = unir_ruta(args.hdfs_uri, args.curated_root)
    for mes in meses:
        ruta_entrada = unir_ruta(
            args.hdfs_uri, f"{args.raw_root}/year={args.year}/month={mes}"
        )
        logger.info("Leyendo datos desde %s", ruta_entrada)

        try:
            df = spark.read.parquet(ruta_entrada)
        except AnalysisException as error:
            if args.skip_missing:
                logger.warning("No se pudo leer %s: %s", ruta_entrada, error)
                continue
            raise
        df = estandarizar_columnas(df, logger=logger)

        requeridas = ["pickup_datetime", "dropoff_datetime", "trip_distance", "fare_amount"]
        validar_columnas(df, requeridas)

        defaults = {
            "total_amount": 0.0,
            "passenger_count": 0,
            "payment_type": "desconocido",
            "pu_location": "desconocido",
            "do_location": "desconocido",
            "vendor_id": "desconocido",
            "ratecode_id": "desconocido",
        }
        df = asegurar_columnas(df, defaults, logger=logger)
        columnas_base = [
            "pickup_datetime",
            "dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "passenger_count",
            "payment_type",
            "pu_location",
            "do_location",
            "vendor_id",
            "ratecode_id",
        ]
        columnas_presentes = [c for c in columnas_base if c in df.columns]
        df = df.select(columnas_presentes)

        df = filtrar_invalidos(df)
        df = agregar_duracion(df)
        df = df.filter(F.col("trip_duration_min") > 0)
        df = agregar_features_temporales(df)

        if args.sample_frac and 0 < args.sample_frac < 1:
            df = df.sample(fraction=args.sample_frac, seed=42)
        if args.max_rows and args.max_rows > 0:
            df = df.limit(args.max_rows)

        if not args.sin_outliers:
            df = filtrar_outliers(
                df,
                columnas=["trip_distance", "fare_amount", "trip_duration_min"],
                p1=args.p1,
                p99=args.p99,
                logger=logger,
            )

        df = agregar_particiones(df, args.year, mes)
        logger.info("Escribiendo datos curated en %s", ruta_salida)

        (
            df.write.mode("overwrite")
            .partitionBy("year", "month")
            .parquet(ruta_salida)
        )

    spark.stop()

if __name__ == "__main__":
    main()
