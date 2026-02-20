![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white&style=for-the-badge)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white&style=for-the-badge)
![Big Data](https://img.shields.io/badge/Big%20Data-Spark%20Cluster-orange?style=for-the-badge)

# microcredit-bigdata-spark-analysis
Analyse Big Data des micro‑crédits Kiva avec Spark (SparkR/pySpark) : nettoyage massif, jointures distribuées, exploration statistique, visualisations, modélisation (régression &amp; classification), comparaison de modèles ML et évaluation des performances sur données volumineuses.
# Analyse Big Data des Micro-Crédits Kiva avec PySpark
Projet réalisé dans le cadre d’un travail Big Data sur données massives Kiva (Kaggle).  
Auteur : Said Ouzzine  
Technologies : PySpark, Spark SQL, MLlib, Python

## 1. Introduction
Ce projet a été entièrement réalisé en PySpark plutôt qu’en SparkR. Python constitue aujourd’hui l’écosystème le plus mature pour le Big Data : PySpark offre un accès complet à l’ensemble des modules Spark (SQL, DataFrames, MLlib), contrairement à SparkR qui reste plus limité, notamment pour la modélisation et les transformations avancées. Python permet également une meilleure intégration avec Pandas et Matplotlib pour l’analyse locale et la visualisation. Enfin, PySpark est largement utilisé dans l’industrie, ce qui en fait un choix naturel pour un projet Big Data professionnel.

L’objectif du projet est d’analyser plusieurs millions de micro-crédits financés par Kiva, en construisant un pipeline Big Data complet : ingestion distribuée, nettoyage, jointures, analyse exploratoire, modélisation et comparaison de modèles Spark MLlib.

## 2. Données utilisées
Les données proviennent du challenge Kaggle "Kiva Crowdfunding Loans".  
Deux fichiers sont utilisés :
- prets.csv : informations détaillées sur chaque prêt
- prets_preteurs.csv : liste des prêteurs associés à chaque prêt

Variables clés : LOAN_AMOUNT, STATUS, SECTOR_NAME, COUNTRY_CODE, LENDER_TERM, NUM_LENDERS_TOTAL, BORROWER_GENDERS, DISTRIBUTION_MODEL.

## 3. Schéma du projet
```
microcredit-bigdata-spark-analysis/
│
├── data/
│   ├── prets.csv
│   └── prets_preteurs.csv
│
├── notebooks/
│   └── bigdata_kiva_analysis.ipynb
│
├── src/
│   ├── 01_ingestion_cleaning.py
│   ├── 02_exploratory_analysis.py
│   ├── 03_feature_engineering.py
│   ├── 04_modeling_regression.py
│   └── 05_modeling_classification.py
│
├── results/
│   ├── eda_outputs/
│   ├── regression_metrics.csv
│   ├── gbt_vs_rf_comparison.csv
│   └── scatterplots/
│
├── README.md
└── requirements.txt
```

## 4. Justification de la structure
La structure adoptée suit les standards des projets Big Data professionnels.  
Elle sépare clairement :

- data/ : données sources, non modifiées, garantissant la reproductibilité.
- notebooks/ : analyses interactives et documentation du pipeline.
- src/ : scripts PySpark organisés par étapes (ETL, EDA, feature engineering, modélisation). Cette modularité permet d’exécuter chaque étape indépendamment et facilite la maintenance.
- results/ : sorties générées (visualisations, métriques, tableaux), permettant de suivre l’évolution des modèles.
- requirements.txt : dépendances nécessaires pour reproduire l’environnement.

Cette organisation reflète une approche robuste, adaptée aux environnements Big Data où les pipelines doivent être reproductibles, scalables et facilement extensibles.

## 5. Pipeline Big Data mis en place

### 5.1 Initialisation Spark
- Configuration Java/Hadoop
- Session Spark optimisée : 6 threads, 12 Go RAM driver, 12 Go RAM executor
- Détection automatique de la version Spark

### 5.2 Ingestion et nettoyage
- Chargement des deux CSV en DataFrames Spark
- Suppression des doublons et valeurs manquantes
- Filtrage des prêts :
  - monnaie = USD
  - statut ∈ {funded, expired}
  - genre ∈ {male, female}
- Vérification des schémas et types

### 5.3 Analyse exploratoire distribuée
- Top 5 des monnaies les plus utilisées
- Top 5 des pays avec le plus de prêts
- Jointure distribuée entre prêts et prêteurs
- Requêtes SQL Spark :
  - Moyenne du nombre de prêteurs
  - Min, moyenne et max du montant des prêts par pays
- Echantillonnage aléatoire (1 %)
- Visualisation : scatter plot LOAN_AMOUNT vs NUM_LENDERS_TOTAL

## 6. Préparation des données pour la modélisation
- Suppression des colonnes LOAN_ID et LENDERS
- Encodage des variables catégorielles :
  - StringIndexer
  - OneHotEncoder
- Construction du vecteur de caractéristiques (VectorAssembler)
- Split train/test : 70 % / 30 %

## 7. Modélisation Spark MLlib

### 7.1 Régression linéaire (modèle de référence)
- RMSE : environ 696
- MAE : environ 362

### 7.2 Random Forest Regressor
Hyperparamètres :
- numTrees = 10
- maxDepth = 10
- maxBins = 16384

### 7.3 Gradient Boosted Trees
Hyperparamètres :
- maxIter = 5
- maxDepth = 10

### 7.4 Comparaison des modèles

| Modèle                     | MAE   | RMSE  |
|---------------------------|-------|-------|
| Régression linéaire       | ~362  | ~696  |
| Random Forest             | >362  | >696  |
| Gradient Boosted Trees    | >362  | meilleur RMSE |

### 7.5 Interprétation des résultats
- La régression linéaire est le modèle le plus stable en MAE.
- Le Gradient Boosted Trees obtient le meilleur RMSE, indiquant une meilleure gestion des valeurs extrêmes.
- Les modèles d’arbres présentent des performances proches mais légèrement inférieures en précision moyenne.

## 8. Technologies utilisées
- PySpark (Spark SQL, DataFrames, MLlib)
- Python 3.10
- Matplotlib
- Pandas
- VS Code

## 9. Auteur
Said Ouzzine  
Data Scientist — Big Data — Machine Learning  
LinkedIn : https://www.linkedin.com/in/said-ouzzine/

