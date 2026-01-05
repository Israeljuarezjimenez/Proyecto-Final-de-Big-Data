from pyspark.sql import functions as F

MAPA_COLUMNAS_TLC = {
    "pickup_datetime": ["tpep_pickup_datetime", "pickup_datetime", "lpep_pickup_datetime"],
    "dropoff_datetime": ["tpep_dropoff_datetime", "dropoff_datetime", "lpep_dropoff_datetime"],
    "trip_distance": ["trip_distance"],
    "fare_amount": ["fare_amount"],
    "total_amount": ["total_amount"],
    "passenger_count": ["passenger_count"],
    "payment_type": ["payment_type"],
    "pu_location": ["PULocationID", "pu_location_id", "pulocationid"],
    "do_location": ["DOLocationID", "do_location_id", "dolocationid"],
    "vendor_id": ["VendorID", "vendorid"],
    "ratecode_id": ["RatecodeID", "ratecodeid"],
}

def estandarizar_columnas(df, mapa=MAPA_COLUMNAS_TLC, logger=None):
    columnas = {c.lower(): c for c in df.columns}
    for objetivo, opciones in mapa.items():
        for opcion in opciones:
            clave = opcion.lower()
            if clave in columnas:
                df = df.withColumnRenamed(columnas[clave], objetivo)
                break
        else:
            if logger:
                logger.warning("Columna no encontrada para %s", objetivo)
    return df

def validar_columnas(df, requeridas):
    faltantes = [c for c in requeridas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Columnas requeridas faltantes: {faltantes}")

def asegurar_columnas(df, defaults, logger=None):
    for columna, valor in defaults.items():
        if columna not in df.columns:
            if logger:
                logger.warning("Agregando columna faltante %s con default", columna)
            df = df.withColumn(columna, F.lit(valor))
    return df

def filtrar_invalidos(df):
    return (
        df.filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
    )

def filtrar_outliers(df, columnas, p1=0.01, p99=0.99, logger=None):
    for columna in columnas:
        if columna not in df.columns:
            if logger:
                logger.warning("Columna no encontrada para outliers: %s", columna)
            continue
        limites = df.stat.approxQuantile(columna, [p1, p99], 0.01)
        if len(limites) != 2:
            if logger:
                logger.warning("No se pudo calcular percentiles para %s", columna)
            continue
        minimo, maximo = limites
        df = df.filter((F.col(columna) >= minimo) & (F.col(columna) <= maximo))
    return df
