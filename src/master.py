"""
master.py
Script principal permettant d'exécuter l'ensemble du pipeline Big Data Kiva.
Auteur : Said Ouzzine

Ce script orchestre les 5 étapes du projet :
1. Ingestion & Nettoyage
2. Analyse exploratoire
3. Feature Engineering
4. Modélisation (régression)
5. Modélisation (classification)

Chaque étape correspond à un module Python séparé dans le dossier src/.
"""

import subprocess
import sys
import os

# -------------------------------------------------------------------
# Fonction utilitaire pour exécuter un script Python
# -------------------------------------------------------------------

def run_script(script_name):
    print("\n====================================================")
    print(f"Exécution du module : {script_name}")
    print("====================================================\n")

    result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)

    # Affichage des sorties
    print(result.stdout)

    # Gestion des erreurs éventuelles
    if result.stderr:
        print("Erreurs détectées :")
        print(result.stderr)

    print("\n----------------------------------------------------")
    print(f"Fin du module : {script_name}")
    print("----------------------------------------------------\n")


# -------------------------------------------------------------------
# 1. Vérification du dossier src/
# -------------------------------------------------------------------

SRC_DIR = os.path.join(os.path.dirname(__file__), "src")

if not os.path.exists(SRC_DIR):
    raise FileNotFoundError("Le dossier 'src/' est introuvable. Vérifiez votre structure GitHub.")

# -------------------------------------------------------------------
# 2. Liste des modules à exécuter dans l'ordre
# -------------------------------------------------------------------

pipeline = [
    "01_ingestion_cleaning.py",
    "02_exploratory_analysis.py",
    "03_feature_engineering.py",
    "04_modeling_regression.py",
    "05_modeling_classification.py"
]

# -------------------------------------------------------------------
# 3. Exécution séquentielle du pipeline
# -------------------------------------------------------------------

print("\n====================================================")
print("DÉMARRAGE DU PIPELINE BIG DATA KIVA")
print("====================================================\n")

for module in pipeline:
    script_path = os.path.join(SRC_DIR, module)
    if not os.path.isfile(script_path):
        raise FileNotFoundError(f"Le script {module} est introuvable dans src/.")
    run_script(script_path)

print("\n====================================================")
print("PIPELINE TERMINÉ AVEC SUCCÈS")
print("====================================================\n")
