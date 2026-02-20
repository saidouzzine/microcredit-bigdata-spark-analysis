"""
05_modeling_classification.py
Classification binaire : prédire si un prêt dépasse la médiane.
Auteur : Said Ouzzine
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier, NaiveBayes, LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("Classification_Kiva").getOrCreate()

# -------------------------------------------------------------------
# 1. Chargement des données
# -------------------------------------------------------------------

df = spark.read.parquet("../data/model_ready.parquet")

# -------------------------------------------------------------------
# 2. Création de la variable binaire
# -------------------------------------------------------------------

median_value = df.approxQuantile("LOAN_AMOUNT", [0.5], 0.01)[0]

df = df.withColumn(
    "pretMedian",
    when(df["LOAN_AMOUNT"] > median_value, 1).otherwise(0)
)

# -------------------------------------------------------------------
# 3. Split train/test
# -------------------------------------------------------------------

train, test = df.randomSplit([0.7, 0.3], seed=123)

# -------------------------------------------------------------------
# 4. Modèles de classification
# -------------------------------------------------------------------

models = {
    "Logistic Regression": LogisticRegression(featuresCol="features", labelCol="pretMedian"),
    "Random Forest": RandomForestClassifier(featuresCol="features", labelCol="pretMedian"),
    "Gradient Boosted Trees": GBTClassifier(featuresCol="features", labelCol="pretMedian"),
    "Naive Bayes": NaiveBayes(featuresCol="features", labelCol="pretMedian"),
    "Linear SVM": LinearSVC(featuresCol="features", labelCol="pretMedian")
}

evaluator = BinaryClassificationEvaluator(labelCol="pretMedian", metricName="areaUnderROC")

# -------------------------------------------------------------------
# 5. Entraînement et évaluation
# -------------------------------------------------------------------

for name, model in models.items():
    print(f"\n--- {name} ---")
    fitted = model.fit(train)
    pred = fitted.transform(test)
    auc = evaluator.evaluate(pred)
    print("AUC :", auc)

print("Classification terminée.")
