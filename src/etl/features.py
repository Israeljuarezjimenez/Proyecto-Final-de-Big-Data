from pyspark.sql import functions as F

def agregar_duracion(df, col_inicio="pickup_datetime", col_fin="dropoff_datetime"):
    return df.withColumn(
        "trip_duration_min",
        (F.unix_timestamp(F.col(col_fin)) - F.unix_timestamp(F.col(col_inicio))) / 60.0,
    )

def agregar_features_temporales(df, col_pickup="pickup_datetime"):
    df = df.withColumn("pickup_hour", F.hour(F.col(col_pickup)))
    df = df.withColumn("pickup_dow", F.dayofweek(F.col(col_pickup)))
    df = df.withColumn("is_weekend", F.col("pickup_dow").isin([1, 7]).cast("int"))
    return df

def agregar_particiones(df, year, month):
    return df.withColumn("year", F.lit(str(year))).withColumn("month", F.lit(str(month)))
