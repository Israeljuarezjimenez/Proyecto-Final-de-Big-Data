from pyspark.sql import SparkSession

def crear_spark(app_name="tlc-pipeline", master=None, config_extra=None, log_level="WARN"):
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config("spark.sql.shuffle.partitions", "8")

    if config_extra:
        for clave, valor in config_extra.items():
            builder = builder.config(clave, valor)

    spark = builder.getOrCreate()
    if log_level:
        spark.sparkContext.setLogLevel(log_level)
    return spark
