"""
03_feature_engineering.py
Encodage des variables catégorielles et préparation des features.
Auteur : Said Ouzzine
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

spark = SparkSession.builder.appName("FeatureEngineering_Kiva").getOrCreate()

# -------------------------------------------------------------------
# 1. Chargement des données jointes
# -------------------------------------------------------------------

pret_joint = spark.read.parquet("../data/sample_1pct.parquet")

# -------------------------------------------------------------------
# 2. Suppression colonnes inutiles
# -------------------------------------------------------------------

pret_joint = pret_joint.drop("LOAN_ID", "LENDERS")

# -------------------------------------------------------------------
# 3. Encodage des variables catégorielles
# -------------------------------------------------------------------

categorical_cols = [col for col, dtype in pret_joint.dtypes if dtype == "string"]

# StringIndexer
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
    for col in categorical_cols
]

for indexer in indexers:
    pret_joint = indexer.fit(pret_joint).transform(pret_joint)

# OneHotEncoder
encoder = OneHotEncoder(
    inputCols=[f"{col}_index" for col in categorical_cols],
    outputCols=[f"{col}_encoded" for col in categorical_cols]
)

pret_joint = encoder.fit(pret_joint).transform(pret_joint)

# -------------------------------------------------------------------
# 4. Construction du vecteur de features
# -------------------------------------------------------------------

encoded_cols = [f"{col}_encoded" for col in categorical_cols]

assembler = VectorAssembler(
    inputCols=encoded_cols,
    outputCol="features"
)

pret_joint = assembler.transform(pret_joint)

# -------------------------------------------------------------------
# 5. Export
# -------------------------------------------------------------------

pret_joint.write.mode("overwrite").parquet("../data/model_ready.parquet")

print("Feature engineering terminé.")
