"""
Module de nettoyage des données entreprises
"""
import pandas as pd
import numpy as np
import re
import hashlib
import logging
from pathlib import Path
from typing import Tuple

DATA_DIR = Path("data/cleaned")

def setup_module_logging():
    return logging.getLogger(__name__)

def clean_company_name(name: str) -> str:
    """Nettoie et normalise le nom d'une entreprise"""
    if pd.isna(name):
        return "Unknown"
    
    # Conversion en string
    name = str(name).strip()
    
    # Suppression des suffixes légaux
    suffixes = [
        r'\s+Inc\.?$', r'\s+LLC$', r'\s+Ltd\.?$', r'\s+GmbH$', 
        r'\s+SA$', r'\s+NV$', r'\s+PLC$', r'\s+Corp\.?$',
        r'\s+Company$', r'\s+Co\.?$', r'\s+& Co\.?$'
    ]
    
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # Normalisation de la ponctuation et des espaces
    name = re.sub(r'[^\w\s&-]', ' ', name)  # Garde & et -
    name = re.sub(r'\s+', ' ', name)
    
    # Titrage (première lettre de chaque mot en majuscule)
    name = name.title()
    
    return name.strip()

def normalize_country(country: str) -> str:
    """Normalise les noms de pays"""
    if pd.isna(country):
        return "Unknown"
    
    country = str(country).strip()
    
    # Mapping des variations
    country_map = {
        'Morocco': ['Morocco', 'MAR', 'Moroccan'],
        'Spain': ['Spain', 'ESP', 'Spanish'],
        'Portugal': ['Portugal', 'PRT', 'Portuguese'],
        'Italy': ['Italy', 'ITA', 'Italian'],
        'France': ['France', 'FRA', 'French'],
        'Greece': ['Greece', 'GRC', 'Greek'],
        'Malta': ['Malta', 'MLT', 'Maltese']
    }
    
    for standard_name, variations in country_map.items():
        if country in variations or country.lower() in [v.lower() for v in variations]:
            return standard_name
    
    return country.title()

def generate_company_id(company_name: str, country: str) -> str:
    """Génère un ID déterministe pour une entreprise"""
    # Création d'une chaîne unique
    unique_string = f"{company_name}_{country}".lower().encode('utf-8')
    
    # Hash MD5 (ou SHA256 pour plus de sécurité)
    hash_obj = hashlib.md5(unique_string)
    
    return f"COMP_{hash_obj.hexdigest()[:12]}"

def clean_companies(input_path: Path) -> Path:
    """Nettoie et normalise les données entreprises"""
    logger = setup_module_logging()
    logger.info(f"Début du nettoyage des entreprises: {input_path}")
    
    # Création du dossier de sortie
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Lecture des données
    df = pd.read_csv(input_path)
    
    # Création du DataFrame entreprises
    companies_df = df[['company_id', 'company_name', 'country']].copy()
    companies_df = companies_df.drop_duplicates(subset=['company_id'])
    
    # Nettoyage des noms
    logger.info("Nettoyage des noms d'entreprises")
    companies_df['company_name_clean'] = companies_df['company_name'].apply(clean_company_name)
    
    # Normalisation des pays
    logger.info("Normalisation des pays")
    companies_df['country_normalized'] = companies_df['country'].apply(normalize_country)
    
    # Génération des IDs
    logger.info("Génération des IDs d'entreprises")
    companies_df['company_id_unified'] = companies_df.apply(
        lambda row: generate_company_id(row['company_name_clean'], row['country_normalized']),
        axis=1
    )
    
    # Sélection des colonnes finales
    final_columns = {
        'company_id_unified': 'company_id',
        'company_name_clean': 'company_name',
        'country_normalized': 'country',
        'original_company_id': 'company_id',
        'original_name': 'company_name'
    }
    
    companies_clean = companies_df.rename(columns=final_columns)
    companies_clean = companies_clean[[
        'company_id', 'company_name', 'country', 
        'original_company_id', 'original_name'
    ]]
    
    # Sauvegarde
    output_path = DATA_DIR / "companies_cleaned.csv"
    companies_clean.to_csv(output_path, index=False, encoding='utf-8')
    
    logger.info(f"Entreprises nettoyées sauvegardées: {output_path}")
    logger.info(f"Nombre d'entreprises: {len(companies_clean)}")
    
    return output_path