import argparse
import os
import sys

from src.spark_session import crear_spark
from src.utils.fechas import resolver_meses
from src.utils.logging import configurar_logging

def unir_ruta(uri, path):
    if uri:
        return uri.rstrip("/") + path
    return path

def main():
    parser = argparse.ArgumentParser(description="Exporta marts para dashboard")
    parser.add_argument("--year", required=True, type=int, help="Anio")
    parser.add_argument("--month", help="Mes (01-12)")
    parser.add_argument("--months", help="Lista de meses separados por coma")
    parser.add_argument("--quarter", type=int, help="Trimestre (1-4)")
    parser.add_argument("--hdfs-uri", default="hdfs://namenode:8020", help="URI HDFS")
    parser.add_argument("--marts-root", default="/data/tlc/marts", help="Ruta marts")
    parser.add_argument(
        "--output-dir", default="data/export", help="Directorio local de salida"
    )
    parser.add_argument(
        "--usar-subdir",
        action="store_true",
        help="Guardar en subdirectorios year=YYYY/month=MM",
    )
    parser.add_argument("--master", default=None, help="Master de Spark")
    parser.add_argument("--app-name", default="tlc-export", help="Nombre de la app")
    args = parser.parse_args()

    logger = configurar_logging("export_tlc")
    spark = crear_spark(app_name=args.app_name, master=args.master)

    tablas = [
        "viajes_por_hora_dia",
        "duracion_promedio_hora",
        "tarifa_promedio_hora",
        "kpis",
    ]

    try:
        meses = resolver_meses(args.month, args.months, args.quarter)
    except ValueError as error:
        logger.error(str(error))
        sys.exit(1)

    usar_subdir = args.usar_subdir or len(meses) > 1
    os.makedirs(args.output_dir, exist_ok=True)
    base_local = os.path.abspath(args.output_dir)

    for mes in meses:
        base_hdfs = unir_ruta(
            args.hdfs_uri, f"{args.marts_root}/year={args.year}/month={mes}"
        )
        if usar_subdir:
            base_destino = os.path.join(base_local, f"year={args.year}", f"month={mes}")
        else:
            base_destino = base_local
        os.makedirs(base_destino, exist_ok=True)

        for tabla in tablas:
            ruta_hdfs = f"{base_hdfs}/{tabla}"
            ruta_local = os.path.join(base_destino, tabla)
            ruta_salida = f"file://{ruta_local}"
            try:
                df = spark.read.parquet(ruta_hdfs)
            except Exception as error:
                logger.warning("No se pudo leer %s: %s", ruta_hdfs, error)
                continue

            logger.info("Exportando %s a %s", tabla, ruta_salida)
            (
                df.coalesce(1)
                .write.mode("overwrite")
                .option("header", "true")
                .csv(ruta_salida)
            )

    spark.stop()

if __name__ == "__main__":
    main()
