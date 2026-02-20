"""
02_exploratory_analysis.py
Analyse exploratoire distribuée des prêts Kiva.
Auteur : Said Ouzzine
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = SparkSession.builder.appName("EDA_Kiva").getOrCreate()

# -------------------------------------------------------------------
# 1. Chargement des données nettoyées
# -------------------------------------------------------------------

prets = spark.read.parquet("../data/prets_clean.parquet")
prets_preteurs = spark.read.parquet("../data/prets_preteurs_clean.parquet")

# -------------------------------------------------------------------
# 2. Top 5 des monnaies
# -------------------------------------------------------------------

top_currencies = (
    prets.select(trim("CURRENCY").alias("CURRENCY"))
         .groupBy("CURRENCY")
         .count()
         .orderBy(col("count").desc())
)

top_currencies.show(5)

# -------------------------------------------------------------------
# 3. Filtrage USD + statuts valides
# -------------------------------------------------------------------

prets_usd = prets.filter(prets["CURRENCY"] == "USD")
prets_status = prets_usd.filter(prets_usd["STATUS"].isin("funded", "expired"))

# -------------------------------------------------------------------
# 4. Top 5 des pays
# -------------------------------------------------------------------

top_countries = (
    prets_status.groupBy("COUNTRY_NAME")
                .count()
                .orderBy(col("count").desc())
)

top_countries.show(5)

# -------------------------------------------------------------------
# 5. Jointure distribuée
# -------------------------------------------------------------------

pret_joint = prets_preteurs.join(prets_status, ["LOAN_ID"], "inner")

# -------------------------------------------------------------------
# 6. Statistiques Spark SQL
# -------------------------------------------------------------------

pret_joint.createOrReplaceTempView("pret_joint_table")

spark.sql("""
    SELECT AVG(NUM_LENDERS_TOTAL) AS avg_lenders
    FROM pret_joint_table
""").show()

spark.sql("""
    SELECT COUNTRY_CODE,
           MIN(LOAN_AMOUNT) AS min_loan,
           AVG(LOAN_AMOUNT) AS avg_loan,
           MAX(LOAN_AMOUNT) AS max_loan
    FROM pret_joint_table
    GROUP BY COUNTRY_CODE
    ORDER BY avg_loan DESC
""").show(20)

# -------------------------------------------------------------------
# 7. Échantillonnage 1%
# -------------------------------------------------------------------

sample = pret_joint.sample(0.01, seed=1512)
sample.write.mode("overwrite").parquet("../data/sample_1pct.parquet")

print("Analyse exploratoire terminée.")
