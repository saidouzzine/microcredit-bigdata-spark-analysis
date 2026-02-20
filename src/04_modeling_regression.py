"""
04_modeling_regression.py
Modélisation du montant des prêts via Spark MLlib.
Auteur : Said Ouzzine
"""

from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("Regression_Kiva").getOrCreate()

# -------------------------------------------------------------------
# 1. Chargement des données prêtes pour la modélisation
# -------------------------------------------------------------------

df = spark.read.parquet("../data/model_ready.parquet")

# -------------------------------------------------------------------
# 2. Split train/test
# -------------------------------------------------------------------

train, test = df.randomSplit([0.7, 0.3], seed=123)

# -------------------------------------------------------------------
# 3. Régression linéaire
# -------------------------------------------------------------------

lr = LinearRegression(featuresCol="features", labelCol="LOAN_AMOUNT")
lr_model = lr.fit(train)
pred_lr = lr_model.transform(test)

# -------------------------------------------------------------------
# 4. Random Forest
# -------------------------------------------------------------------

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="LOAN_AMOUNT",
    numTrees=10,
    maxDepth=10,
    maxBins=16384
)

rf_model = rf.fit(train)
pred_rf = rf_model.transform(test)

# -------------------------------------------------------------------
# 5. Gradient Boosted Trees
# -------------------------------------------------------------------

gbt = GBTRegressor(
    featuresCol="features",
    labelCol="LOAN_AMOUNT",
    maxIter=5,
    maxDepth=10
)

gbt_model = gbt.fit(train)
pred_gbt = gbt_model.transform(test)

# -------------------------------------------------------------------
# 6. Évaluation
# -------------------------------------------------------------------

evaluator_rmse = RegressionEvaluator(labelCol="LOAN_AMOUNT", predictionCol="prediction", metricName="rmse")
evaluator_mae = RegressionEvaluator(labelCol="LOAN_AMOUNT", predictionCol="prediction", metricName="mae")

print("RMSE LR :", evaluator_rmse.evaluate(pred_lr))
print("MAE  LR :", evaluator_mae.evaluate(pred_lr))

print("RMSE RF :", evaluator_rmse.evaluate(pred_rf))
print("MAE  RF :", evaluator_mae.evaluate(pred_rf))

print("RMSE GBT :", evaluator_rmse.evaluate(pred_gbt))
print("MAE  GBT :", evaluator_mae.evaluate(pred_gbt))

print("Modélisation régression terminée.")
