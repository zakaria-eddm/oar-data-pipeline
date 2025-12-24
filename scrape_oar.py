"""
Module d'extraction des données OAR
"""
import requests
import pandas as pd
import logging
from pathlib import Path
from typing import Optional
import time

# Configuration
COUNTRIES = ['Morocco', 'Spain', 'Portugal', 'Italy', 'France', 'Greece', 'Malta']
MIN_COMPANIES = 10000
OAR_API_URL = "https://openapparel.org/api/facilities"
DATA_DIR = Path("data/raw")

def setup_module_logging():
    """Configuration du logging pour ce module"""
    return logging.getLogger(__name__)

def download_oar_data() -> Path:
    """
    Télécharge les données OAR et les filtre par pays
    
    Returns:
        Path: Chemin vers le fichier brut sauvegardé
    """
    logger = setup_module_logging()
    logger.info("Début du téléchargement des données OAR")
    
    # Création du dossier de données
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        # Téléchargement via API
        logger.info("Téléchargement des données depuis l'API OAR...")
        response = requests.get(OAR_API_URL, params={"format": "json"})
        response.raise_for_status()
        
        data = response.json()
        df = pd.DataFrame(data['features'])
        
        # Extraction des colonnes pertinentes
        records = []
        for feature in data['features']:
            props = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            record = {
                'id': props.get('os_id'),
                'name': props.get('name'),
                'address': props.get('address'),
                'country': props.get('country'),
                'lat': geometry.get('coordinates', [None, None])[1],
                'lon': geometry.get('coordinates', [None, None])[0],
                'is_closed': props.get('is_closed', False),
                'created_at': props.get('created_at'),
                'updated_at': props.get('updated_at'),
                'contributor': props.get('contributor'),
                'sector': props.get('sector'),
                'processing_activity': props.get('processing_activity')
            }
            
            # Extraction des infos entreprise
            contributors = props.get('contributors', [])
            if contributors:
                record['company_name'] = contributors[0].get('name')
                record['company_id'] = contributors[0].get('id')
            
            records.append(record)
        
        df = pd.DataFrame(records)
        
        # Filtrage par pays
        logger.info(f"Filtrage des données pour {len(COUNTRIES)} pays")
        df_filtered = df[df['country'].isin(COUNTRIES)].copy()
        
        # Vérification du nombre minimum d'entreprises
        if df_filtered['company_id'].nunique() < MIN_COMPANIES:
            logger.warning(f"Seulement {df_filtered['company_id'].nunique()} entreprises trouvées")
            # On pourrait ici ajouter une logique pour télécharger plus de données
        
        # Sauvegarde
        output_path = DATA_DIR / f"oar_raw_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
        df_filtered.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Données sauvegardées: {output_path}")
        logger.info(f"Statistiques: {len(df_filtered)} établissements, {df_filtered['company_id'].nunique()} entreprises")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement: {str(e)}")
        raise

# Alternative: Téléchargement depuis CSV bulk
def download_from_bulk() -> Path:
    """Alternative: Téléchargement depuis l'export CSV bulk"""
    logger = setup_module_logging()
    
    bulk_url = "https://openapparel.org/api/facilities.csv"
    
    try:
        logger.info("Téléchargement du CSV bulk...")
        df = pd.read_csv(bulk_url, low_memory=False)
        
        # Filtrage par pays
        df_filtered = df[df['country'].isin(COUNTRIES)].copy()
        
        output_path = DATA_DIR / f"oar_bulk_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
        df_filtered.to_csv(output_path, index=False)
        
        return output_path
        
    except Exception as e:
        logger.error(f"Erreur avec le bulk CSV: {str(e)}")
        raise