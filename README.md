#  OAR Data Science Pipeline

Pipeline de données complet pour l'analyse du registre Open Apparel Registry (OAR).

##  Description
Ce projet implémente un pipeline ETL (Extract, Transform, Load) complet pour analyser les données de la chaîne d'approvisionnement de l'industrie textile, en suivant les spécifications du test technique CommonShare.

##  Architecture du Pipeline
Le pipeline exécute séquentiellement 7 phases :
1. **Extraction** (`scrape_oar.py`) - Téléchargement des données OAR
2. **Nettoyage entreprises** (`clean_companies.py`) - Normalisation et standardisation
3. **Traitement établissements** (`clean_facilities.py`) - Extraction et nettoyage
4. **Structuration** (`relational_builder.py`) - Création de tables relationnelles
5. **Analytics** (`analytics_dashboards.py`) - Visualisations et statistiques
6. **IA** (`ai_module.py`) - Analyse de durabilité (règle-based)
7. **Export** (`export_final.py`) - Génération de rapports finaux

##  Installation et Exécution

### Prérequis
- Python 3.8+
- pip (gestionnaire de paquets Python)

### Installation
```bash
# 1. Cloner le dépôt
git clone https://github.com/USERNAME/oar-data-pipeline.git
cd oar-data-pipeline

# 2. Créer un environnement virtuel
python -m venv .venv
source .venv/bin/activate  # Sur Linux/Mac
# ou .venv\Scripts\activate  # Sur Windows

# 3. Installer les dépendances
pip install -r requirements.txt
