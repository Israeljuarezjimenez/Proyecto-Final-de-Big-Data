import argparse
import json
import os
import shutil
import sys
import urllib.request
from datetime import datetime, timezone

from src.utils.logging import configurar_logging
from src.utils.fechas import resolver_meses

def descargar_archivo(url, destino, logger):
    logger.info("Descargando %s", url)
    with urllib.request.urlopen(url) as respuesta, open(destino, "wb") as archivo:
        shutil.copyfileobj(respuesta, archivo)
    logger.info("Archivo guardado en %s", destino)

def obtener_columnas_parquet(ruta, logger):
    try:
        import pyarrow.parquet as pq

        esquema = pq.ParquetFile(ruta).schema
        return esquema.names
    except Exception as error_pyarrow:
        logger.warning("pyarrow no disponible o fallo: %s", error_pyarrow)

    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("manifest_tlc")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        columnas = spark.read.parquet(ruta).columns
        spark.stop()
        return columnas
    except Exception as error_spark:
        logger.warning("No se pudo leer parquet con Spark: %s", error_spark)

    return []

def actualizar_manifest(ruta_manifest, registro, logger):
    data = []
    if os.path.exists(ruta_manifest):
        with open(ruta_manifest, "r", encoding="utf-8") as archivo:
            try:
                data = json.load(archivo)
            except json.JSONDecodeError:
                logger.warning("Manifest existente no es JSON valido, se reemplaza")
                data = []

    data = [r for r in data if r.get("nombre_archivo") != registro["nombre_archivo"]]
    data.append(registro)

    with open(ruta_manifest, "w", encoding="utf-8") as archivo:
        json.dump(data, archivo, indent=2, ensure_ascii=True)


def main():
    parser = argparse.ArgumentParser(description="Descarga datos TLC en Parquet")
    parser.add_argument("--year", required=True, type=int, help="Anio de los datos")
    parser.add_argument("--month", help="Mes (01-12)")
    parser.add_argument("--months", help="Lista de meses separados por coma")
    parser.add_argument("--quarter", type=int, help="Trimestre (1-4)")
    parser.add_argument(
        "--base-url",
        default="https://d37ci6vzurychx.cloudfront.net/trip-data",
        help="Base URL del dataset",
    )
    parser.add_argument(
        "--output-dir", default="data/raw", help="Directorio local de salida"
    )
    parser.add_argument(
        "--manifest-path",
        default=None,
        help="Ruta del manifest.json (default: output-dir/manifest.json)",
    )
    parser.add_argument("--force", action="store_true", help="Forzar descarga")
    args = parser.parse_args()

    logger = configurar_logging("download_tlc")
    try:
        meses = resolver_meses(args.month, args.months, args.quarter)
    except ValueError as error:
        logger.error(str(error))
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)
    ruta_manifest = args.manifest_path or os.path.join(args.output_dir, "manifest.json")

    for mes in meses:
        nombre_archivo = f"yellow_tripdata_{args.year}-{mes}.parquet"
        url = f"{args.base_url}/{nombre_archivo}"
        ruta_salida = os.path.join(args.output_dir, nombre_archivo)

        if os.path.exists(ruta_salida) and not args.force:
            logger.info("Archivo ya existe, usa --force para re-descargar: %s", ruta_salida)
        else:
            descargar_archivo(url, ruta_salida, logger)

        columnas = obtener_columnas_parquet(ruta_salida, logger)
        tamano = os.path.getsize(ruta_salida)
        fecha_descarga = datetime.now(timezone.utc).isoformat()

        registro = {
            "nombre_archivo": nombre_archivo,
            "tamano_bytes": tamano,
            "columnas": columnas,
            "fecha_descarga": fecha_descarga,
            "year": str(args.year),
            "month": str(mes),
        }

        actualizar_manifest(ruta_manifest, registro, logger)

    logger.info("Manifest actualizado en %s", ruta_manifest)

if __name__ == "__main__":
    main()
