from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor

def crear_pipeline(columnas_numericas, columnas_categoricas, algoritmo="gbt", col_label="trip_duration_min"):
    etapas = []
    columnas_oh = []

    for columna in columnas_categoricas:
        idx = StringIndexer(
            inputCol=columna,
            outputCol=f"{columna}_idx",
            handleInvalid="keep",
        )
        oh = OneHotEncoder(
            inputCol=f"{columna}_idx",
            outputCol=f"{columna}_oh",
        )
        etapas.extend([idx, oh])
        columnas_oh.append(f"{columna}_oh")

    assembler = VectorAssembler(
        inputCols=columnas_numericas + columnas_oh,
        outputCol="features",
        handleInvalid="keep",
    )
    etapas.append(assembler)

    if algoritmo == "rf":
        modelo = RandomForestRegressor(
            labelCol=col_label,
            featuresCol="features",
            numTrees=100,
            maxDepth=10,
            seed=42,
        )
    else:
        modelo = GBTRegressor(
            labelCol=col_label,
            featuresCol="features",
            maxIter=50,
            maxDepth=5,
            seed=42,
        )

    etapas.append(modelo)
    return Pipeline(stages=etapas)
