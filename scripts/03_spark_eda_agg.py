import argparse
import sys

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

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
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Omitir meses que no existan en HDFS",
    )
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
        try:
            df = spark.read.parquet(ruta_entrada)
        except AnalysisException as error:
            if args.skip_missing:
                logger.warning("No se pudo leer %s: %s", ruta_entrada, error)
                continue
            raise

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

        top_origen = (
            df.groupBy("pu_location")
            .agg(
                F.count("*").alias("total_viajes"),
                F.avg("trip_distance").alias("distancia_promedio"),
                F.avg("trip_duration_min").alias("duracion_promedio_min"),
                F.avg(col_tarifa).alias("tarifa_promedio"),
            )
            .orderBy(F.desc("total_viajes"))
            .limit(20)
        )

        top_destino = (
            df.groupBy("do_location")
            .agg(
                F.count("*").alias("total_viajes"),
                F.avg("trip_distance").alias("distancia_promedio"),
                F.avg("trip_duration_min").alias("duracion_promedio_min"),
                F.avg(col_tarifa).alias("tarifa_promedio"),
            )
            .orderBy(F.desc("total_viajes"))
            .limit(20)
        )

        pagos = (
            df.groupBy("payment_type")
            .agg(
                F.count("*").alias("total_viajes"),
                F.avg("trip_duration_min").alias("duracion_promedio_min"),
                F.avg(col_tarifa).alias("tarifa_promedio"),
            )
            .orderBy(F.desc("total_viajes"))
        )

        vendor = (
            df.groupBy("vendor_id")
            .agg(
                F.count("*").alias("total_viajes"),
                F.avg("trip_duration_min").alias("duracion_promedio_min"),
                F.avg(col_tarifa).alias("tarifa_promedio"),
            )
            .orderBy(F.desc("total_viajes"))
        )

        distancia_bins = (
            df.withColumn(
                "distancia_bin",
                F.when(F.col("trip_distance") < 1, "0-1")
                .when(F.col("trip_distance") < 3, "1-3")
                .when(F.col("trip_distance") < 5, "3-5")
                .when(F.col("trip_distance") < 10, "5-10")
                .when(F.col("trip_distance") < 20, "10-20")
                .otherwise("20+"),
            )
            .withColumn(
                "distancia_orden",
                F.when(F.col("trip_distance") < 1, 1)
                .when(F.col("trip_distance") < 3, 2)
                .when(F.col("trip_distance") < 5, 3)
                .when(F.col("trip_distance") < 10, 4)
                .when(F.col("trip_distance") < 20, 5)
                .otherwise(6),
            )
            .groupBy("distancia_bin", "distancia_orden")
            .agg(
                F.count("*").alias("total_viajes"),
                F.avg("trip_distance").alias("distancia_promedio"),
                F.avg("trip_duration_min").alias("duracion_promedio_min"),
                F.avg(col_tarifa).alias("tarifa_promedio"),
            )
            .orderBy("distancia_orden")
        )

        variabilidad_hora = (
            df.groupBy("pickup_hour")
            .agg(
                F.count("*").alias("total_viajes"),
                F.avg("trip_duration_min").alias("duracion_promedio_min"),
                F.stddev("trip_duration_min").alias("duracion_std"),
                F.expr(
                    "percentile_approx(trip_duration_min, array(0.25, 0.5, 0.75))"
                ).alias("duracion_percentiles"),
                F.avg(col_tarifa).alias("tarifa_promedio"),
                F.stddev(col_tarifa).alias("tarifa_std"),
                F.expr(
                    f"percentile_approx({col_tarifa}, array(0.25, 0.5, 0.75))"
                ).alias("tarifa_percentiles"),
            )
            .orderBy("pickup_hour")
        )

        variabilidad_hora = (
            variabilidad_hora.withColumn(
                "duracion_p25", F.col("duracion_percentiles").getItem(0)
            )
            .withColumn("duracion_p50", F.col("duracion_percentiles").getItem(1))
            .withColumn("duracion_p75", F.col("duracion_percentiles").getItem(2))
            .withColumn("tarifa_p25", F.col("tarifa_percentiles").getItem(0))
            .withColumn("tarifa_p50", F.col("tarifa_percentiles").getItem(1))
            .withColumn("tarifa_p75", F.col("tarifa_percentiles").getItem(2))
            .drop("duracion_percentiles", "tarifa_percentiles")
        )

        variabilidad_dia = (
            df.groupBy("pickup_dow")
            .agg(
                F.count("*").alias("total_viajes"),
                F.avg("trip_duration_min").alias("duracion_promedio_min"),
                F.stddev("trip_duration_min").alias("duracion_std"),
                F.expr(
                    "percentile_approx(trip_duration_min, array(0.25, 0.5, 0.75))"
                ).alias("duracion_percentiles"),
                F.avg(col_tarifa).alias("tarifa_promedio"),
                F.stddev(col_tarifa).alias("tarifa_std"),
                F.expr(
                    f"percentile_approx({col_tarifa}, array(0.25, 0.5, 0.75))"
                ).alias("tarifa_percentiles"),
            )
            .orderBy("pickup_dow")
        )

        variabilidad_dia = (
            variabilidad_dia.withColumn(
                "duracion_p25", F.col("duracion_percentiles").getItem(0)
            )
            .withColumn("duracion_p50", F.col("duracion_percentiles").getItem(1))
            .withColumn("duracion_p75", F.col("duracion_percentiles").getItem(2))
            .withColumn("tarifa_p25", F.col("tarifa_percentiles").getItem(0))
            .withColumn("tarifa_p50", F.col("tarifa_percentiles").getItem(1))
            .withColumn("tarifa_p75", F.col("tarifa_percentiles").getItem(2))
            .drop("duracion_percentiles", "tarifa_percentiles")
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
        top_origen.write.mode("overwrite").parquet(f"{ruta_salida}/top_origen")
        top_destino.write.mode("overwrite").parquet(f"{ruta_salida}/top_destino")
        pagos.write.mode("overwrite").parquet(f"{ruta_salida}/pagos")
        vendor.write.mode("overwrite").parquet(f"{ruta_salida}/vendor")
        distancia_bins.write.mode("overwrite").parquet(f"{ruta_salida}/distancia_bins")
        variabilidad_hora.write.mode("overwrite").parquet(
            f"{ruta_salida}/variabilidad_hora"
        )
        variabilidad_dia.write.mode("overwrite").parquet(
            f"{ruta_salida}/variabilidad_dia"
        )

    spark.stop()

if __name__ == "__main__":
    main()
