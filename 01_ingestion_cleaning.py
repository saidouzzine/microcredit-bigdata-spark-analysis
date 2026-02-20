"""
01_ingestion_cleaning.py
Ingestion des données Kiva et nettoyage initial.
Auteur : Said Ouzzine
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, count, isnull, when, trim

# -------------------------------------------------------------------
# 1. Configuration de l'environnement Spark
# -------------------------------------------------------------------

java_home_path = r'C:\Program Files\Java\jdk-17'
hadoop_home_path = r'C:\hadoop'

os.environ['PATH'] = f'{java_home_path}\\bin;{hadoop_home_path}\\bin;{hadoop_home_path}\\sbin;' + os.environ['PATH']

spark = SparkSession.builder \
       .master("local[6]") \
       .appName("BigData_Kiva") \
       .config("spark.driver.memory", "12g") \
       .config("spark.executor.memory", "12g") \
       .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

print("Version Spark :", spark.version)

# -------------------------------------------------------------------
# 2. Chargement des fichiers CSV
# -------------------------------------------------------------------

prets_preteurs = spark.read.csv("prets_preteurs.csv", header=True, inferSchema=True)
prets = spark.read.csv("prets.csv", header=True, inferSchema=True)

# -------------------------------------------------------------------
# 3. Sélection des colonnes pertinentes
# -------------------------------------------------------------------

prets = prets.select(
    "LOAN_ID", "LOAN_AMOUNT", "STATUS", "SECTOR_NAME", "COUNTRY_CODE",
    "LENDER_TERM", "NUM_LENDERS_TOTAL", "BORROWER_GENDERS", "DISTRIBUTION_MODEL",
    "COUNTRY_NAME", "CURRENCY"
)

# -------------------------------------------------------------------
# 4. Nettoyage : doublons + valeurs manquantes
# -------------------------------------------------------------------

prets = prets.dropDuplicates()
prets = prets.na.drop()

# Vérification des valeurs nulles restantes
null_counts = prets.select([count(when(isnull(c), c)).alias(c) for c in prets.columns])
null_counts.show()

# -------------------------------------------------------------------
# 5. Filtrage : prêts valides
# -------------------------------------------------------------------

prets = prets.filter(
    (prets["COUNTRY_CODE"].isNotNull()) &
    (prets["BORROWER_GENDERS"].isin("male", "female"))
)

# -------------------------------------------------------------------
# 6. Export pour les étapes suivantes
# -------------------------------------------------------------------

prets.write.mode("overwrite").parquet("../data/prets_clean.parquet")
prets_preteurs.write.mode("overwrite").parquet("../data/prets_preteurs_clean.parquet")

print("Ingestion et nettoyage terminés.")
