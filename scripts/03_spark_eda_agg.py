import argparse
import sys

from pyspark.sql import functions as F

from src.spark_session import crear_spark
from src.utils.fechas import resolver_meses
from src.utils.logging import configurar_logging

def unir_ruta(uri, path):
    if uri:
        return uri.rstrip("/") + path
    return path

def main():
    parser = argparse.ArgumentParser(description="EDA y agregaciones TLC")
    parser.add_argument("--year", required=True, type=int, help="Anio")
    parser.add_argument("--month", help="Mes (01-12)")
    parser.add_argument("--months", help="Lista de meses separados por coma")
    parser.add_argument("--quarter", type=int, help="Trimestre (1-4)")
    parser.add_argument("--hdfs-uri", default="hdfs://namenode:8020", help="URI HDFS")
    parser.add_argument("--curated-root", default="/data/tlc/curated", help="Ruta curated")
    parser.add_argument("--marts-root", default="/data/tlc/marts", help="Ruta marts")
    parser.add_argument("--master", default=None, help="Master de Spark")
    parser.add_argument("--app-name", default="tlc-eda", help="Nombre de la app")
    args = parser.parse_args()

    logger = configurar_logging("eda_tlc")
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

        col_tarifa = "total_amount" if "total_amount" in df.columns else "fare_amount"

        viajes_por_hora_dia = (
            df.groupBy("pickup_hour", "pickup_dow")
            .agg(F.count("*").alias("total_viajes"))
            .orderBy("pickup_dow", "pickup_hour")
        )

        duracion_promedio_hora = (
            df.groupBy("pickup_hour")
            .agg(F.avg("trip_duration_min").alias("duracion_promedio_min"))
            .orderBy("pickup_hour")
        )

        tarifa_promedio_hora = (
            df.groupBy("pickup_hour")
            .agg(F.avg(col_tarifa).alias("tarifa_promedio"))
            .orderBy("pickup_hour")
        )

        kpis = df.agg(
            F.count("*").alias("total_viajes"),
            F.avg("trip_duration_min").alias("duracion_promedio_min"),
            F.avg(col_tarifa).alias("tarifa_promedio"),
        )

        ruta_salida = unir_ruta(
            args.hdfs_uri, f"{args.marts_root}/year={args.year}/month={mes}"
        )
        logger.info("Escribiendo marts en %s", ruta_salida)

        viajes_por_hora_dia.write.mode("overwrite").parquet(
            f"{ruta_salida}/viajes_por_hora_dia"
        )
        duracion_promedio_hora.write.mode("overwrite").parquet(
            f"{ruta_salida}/duracion_promedio_hora"
        )
        tarifa_promedio_hora.write.mode("overwrite").parquet(
            f"{ruta_salida}/tarifa_promedio_hora"
        )
        kpis.coalesce(1).write.mode("overwrite").parquet(f"{ruta_salida}/kpis")

    spark.stop()

if __name__ == "__main__":
    main()
