from pyspark.ml.evaluation import RegressionEvaluator

def calcular_metricas_regresion(df, label_col="trip_duration_min", pred_col="prediction"):
    evaluador = RegressionEvaluator(labelCol=label_col, predictionCol=pred_col)
    rmse = evaluador.setMetricName("rmse").evaluate(df)
    mae = evaluador.setMetricName("mae").evaluate(df)
    r2 = evaluador.setMetricName("r2").evaluate(df)
    return {"rmse": rmse, "mae": mae, "r2": r2}
