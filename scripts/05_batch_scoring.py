import argparse
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from src.spark_session import crear_spark
from src.utils.fechas import resolver_meses
from src.utils.logging import configurar_logging

def unir_ruta(uri, path):
    if uri:
        return uri.rstrip("/") + path
    return path

def main():
    parser = argparse.ArgumentParser(description="Batch scoring TLC")
    parser.add_argument("--year", required=True, type=int, help="Anio")
    parser.add_argument("--month", help="Mes (01-12)")
    parser.add_argument("--months", help="Lista de meses separados por coma")
    parser.add_argument("--quarter", type=int, help="Trimestre (1-4)")
    parser.add_argument("--hdfs-uri", default="hdfs://namenode:8020", help="URI HDFS")
    parser.add_argument("--curated-root", default="/data/tlc/curated", help="Ruta curated")
    parser.add_argument(
        "--model-root", default="/models/tlc_trip_duration", help="Ruta modelo"
    )
    parser.add_argument(
        "--predictions-root",
        default="/data/tlc/predictions",
        help="Ruta predictions",
    )
    parser.add_argument(
        "--sample-frac",
        type=float,
        default=None,
        help="Fraccion de muestreo para scoring",
    )
    parser.add_argument(
        "--max-rows", type=int, default=None, help="Limite de filas para scoring"
    )
    parser.add_argument("--master", default=None, help="Master de Spark")
    parser.add_argument("--app-name", default="tlc-scoring", help="Nombre de la app")
    args = parser.parse_args()

    logger = configurar_logging("scoring_tlc")
    spark = crear_spark(app_name=args.app_name, master=args.master)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        meses = resolver_meses(args.month, args.months, args.quarter)
    except ValueError as error:
        logger.error(str(error))
        sys.exit(1)

    ruta_salida = unir_ruta(args.hdfs_uri, args.predictions_root)
    for mes in meses:
        ruta_datos = unir_ruta(
            args.hdfs_uri, f"{args.curated_root}/year={args.year}/month={mes}"
        )
        ruta_modelo = unir_ruta(
            args.hdfs_uri, f"{args.model_root}/year={args.year}/month={mes}"
        )

        logger.info("Leyendo datos curated desde %s", ruta_datos)
        df = spark.read.parquet(ruta_datos)
        if args.sample_frac and 0 < args.sample_frac < 1:
            df = df.sample(fraction=args.sample_frac, seed=42)
        if args.max_rows and args.max_rows > 0:
            df = df.limit(args.max_rows)

        logger.info("Cargando modelo desde %s", ruta_modelo)
        modelo = PipelineModel.load(ruta_modelo)

        pred = modelo.transform(df)

        if "year" not in pred.columns:
            pred = pred.withColumn("year", F.lit(str(args.year)))
        if "month" not in pred.columns:
            pred = pred.withColumn("month", F.lit(str(mes)))

        logger.info("Escribiendo predicciones en %s", ruta_salida)

        (
            pred.write.mode("overwrite")
            .partitionBy("year", "month")
            .parquet(ruta_salida)
        )

    spark.stop()

if __name__ == "__main__":
    main()
