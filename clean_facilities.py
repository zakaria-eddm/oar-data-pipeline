"""
Module de traitement des établissements
"""
import pandas as pd
import numpy as np
import re
import hashlib
import logging
from pathlib import Path
from typing import Dict, Tuple

DATA_DIR = Path("data/cleaned")

def setup_module_logging():
    return logging.getLogger(__name__)

def clean_facility_name(name: str) -> str:
    """Nettoie le nom d'un établissement"""
    if pd.isna(name):
        return "Unknown Facility"
    
    name = str(name).strip()
    
    # Suppression des caractères spéciaux problématiques
    name = re.sub(r'[^\w\s\-\'&.,()]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    
    return name.title()

def generate_facility_id(facility_name: str, lat: float, lon: float) -> str:
    """Génère un ID unique pour un établissement"""
    if pd.isna(lat) or pd.isna(lon):
        unique_string = facility_name.lower().encode('utf-8')
    else:
        unique_string = f"{facility_name}_{lat:.4f}_{lon:.4f}".lower().encode('utf-8')
    
    hash_obj = hashlib.md5(unique_string)
    return f"FAC_{hash_obj.hexdigest()[:12]}"

def process_facilities(cleaned_companies_path: Path) -> Dict[str, Path]:
    """Traite les données des établissements"""
    logger = setup_module_logging()
    logger.info(f"Traitement des établissements pour: {cleaned_companies_path}")
    
    # Lecture des données originales et des entreprises nettoyées
    raw_df = pd.read_csv("data/raw/oar_raw_*.csv" if "raw" in cleaned_companies_path.parent.name else cleaned_companies_path)
    companies_df = pd.read_csv(cleaned_companies_path)
    
    # Préparation des données établissements
    facilities_data = []
    
    for _, row in raw_df.iterrows():
        facility_record = {
            'original_id': row.get('id'),
            'original_name': row.get('name'),
            'address': row.get('address'),
            'original_country': row.get('country'),
            'lat': row.get('lat'),
            'lon': row.get('lon'),
            'is_closed': row.get('is_closed', False),
            'sector': row.get('sector'),
            'processing_activity': row.get('processing_activity'),
            'contributor': row.get('contributor'),
            'created_at': row.get('created_at'),
            'updated_at': row.get('updated_at'),
            'original_company_id': row.get('company_id'),
            'original_company_name': row.get('company_name')
        }
        
        # Recherche de l'entreprise correspondante
        company_match = companies_df[
            companies_df['original_company_id'] == row.get('company_id')
        ]
        
        if not company_match.empty:
            facility_record['company_id'] = company_match.iloc[0]['company_id']
            facility_record['company_name'] = company_match.iloc[0]['company_name']
        else:
            facility_record['company_id'] = None
            facility_record['company_name'] = None
        
        facilities_data.append(facility_record)
    
    facilities_df = pd.DataFrame(facilities_data)
    
    # Nettoyage des noms d'établissements
    logger.info("Nettoyage des noms d'établissements")
    facilities_df['facility_name_clean'] = facilities_df['original_name'].apply(clean_facility_name)
    
    # Génération des IDs d'établissements
    logger.info("Génération des IDs d'établissements")
    facilities_df['facility_id'] = facilities_df.apply(
        lambda row: generate_facility_id(
            row['facility_name_clean'], 
            row['lat'], 
            row['lon']
        ),
        axis=1
    )
    
    # Table des établissements
    facilities_table = facilities_df[[
        'facility_id', 'facility_name_clean', 'address',
        'original_country', 'lat', 'lon', 'is_closed',
        'sector', 'processing_activity', 'contributor',
        'created_at', 'updated_at', 'original_id'
    ]].rename(columns={'facility_name_clean': 'facility_name'})
    
    # Table de liaison entreprises-établissements
    links_table = facilities_df[['company_id', 'facility_id']].dropna()
    
    # Sauvegarde
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    facilities_path = DATA_DIR / "facilities.csv"
    links_path = DATA_DIR / "company_facilities_links.csv"
    
    facilities_table.to_csv(facilities_path, index=False)
    links_table.to_csv(links_path, index=False)
    
    logger.info(f"Établissements sauvegardés: {facilities_path}")
    logger.info(f"Liens sauvegardés: {links_path}")
    logger.info(f"Nombre d'établissements: {len(facilities_table)}")
    logger.info(f"Nombre de liens: {len(links_table)}")
    
    return {
        'companies': cleaned_companies_path,
        'facilities': facilities_path,
        'links': links_path
    }