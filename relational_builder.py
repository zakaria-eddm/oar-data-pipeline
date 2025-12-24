"""
Module de construction des tables relationnelles
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict

RELATIONAL_DIR = Path("data/relational")

def setup_module_logging():
    return logging.getLogger(__name__)

def validate_relational_integrity(companies: pd.DataFrame, 
                                  facilities: pd.DataFrame, 
                                  links: pd.DataFrame) -> bool:
    """Valide l'intégrité relationnelle des données"""
    logger = setup_module_logging()
    
    errors = []
    
    # Vérification 1: Pas d'entreprise orpheline
    linked_company_ids = set(links['company_id'].unique())
    all_company_ids = set(companies['company_id'].unique())
    
    orphaned_companies = all_company_ids - linked_company_ids
    if orphaned_companies:
        errors.append(f"Entreprises orphelines: {len(orphaned_companies)}")
    
    # Vérification 2: Pas d'établissement orphelin
    linked_facility_ids = set(links['facility_id'].unique())
    all_facility_ids = set(facilities['facility_id'].unique())
    
    orphaned_facilities = all_facility_ids - linked_facility_ids
    if orphaned_facilities:
        errors.append(f"Établissements orphelins: {len(orphaned_facilities)}")
    
    # Vérification 3: Cohérence des IDs
    for company_id in links['company_id'].unique():
        if company_id not in all_company_ids:
            errors.append(f"ID entreprise inconnu dans les liens: {company_id}")
    
    for facility_id in links['facility_id'].unique():
        if facility_id not in all_facility_ids:
            errors.append(f"ID établissement inconnu dans les liens: {facility_id}")
    
    if errors:
        for error in errors:
            logger.error(error)
        return False
    
    logger.info("Validation relationnelle réussie")
    return True

def build_relational_tables(companies_path: Path, 
                            facilities_path: Path, 
                            links_path: Path) -> Dict[str, Path]:
    """Construit et valide les tables relationnelles"""
    logger = setup_module_logging()
    logger.info("Construction des tables relationnelles")
    
    # Création du dossier
    RELATIONAL_DIR.mkdir(parents=True, exist_ok=True)
    
    # Lecture des données
    companies = pd.read_csv(companies_path)
    facilities = pd.read_csv(facilities_path)
    links = pd.read_csv(links_path)
    
    # Validation
    if not validate_relational_integrity(companies, facilities, links):
        logger.warning("Problèmes d'intégrité détectés")
    
    # Nettoyage des liens (suppression des références manquantes)
    valid_links = links[
        links['company_id'].isin(companies['company_id']) & 
        links['facility_id'].isin(facilities['facility_id'])
    ]
    
    # Sauvegarde des tables relationnelles
    companies_relational = companies[['company_id', 'company_name', 'country']].copy()
    facilities_relational = facilities[['facility_id', 'facility_name', 'lat', 'lon', 'country']].copy()
    
    companies_output = RELATIONAL_DIR / "companies_relational.csv"
    facilities_output = RELATIONAL_DIR / "facilities_relational.csv"
    links_output = RELATIONAL_DIR / "company_facilities_relational.csv"
    
    companies_relational.to_csv(companies_output, index=False)
    facilities_relational.to_csv(facilities_output, index=False)
    valid_links.to_csv(links_output, index=False)
    
    logger.info(f"Tables relationnelles sauvegardées dans: {RELATIONAL_DIR}")
    logger.info(f"- Companies: {len(companies_relational)} lignes")
    logger.info(f"- Facilities: {len(facilities_relational)} lignes")
    logger.info(f"- Links: {len(valid_links)} lignes")
    
    return {
        'companies': companies_output,
        'facilities': facilities_output,
        'links': links_output
    }