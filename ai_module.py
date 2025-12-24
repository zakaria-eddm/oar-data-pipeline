"""
Module d'analyse IA - Détection de durabilité
"""
import pandas as pd
import re
import logging
from pathlib import Path
from typing import List, Dict

OUTPUTS_DIR = Path("data/outputs")

def setup_module_logging():
    return logging.getLogger(__name__)

def detect_sustainability_keywords(text: str, keywords: List[str]) -> Dict:
    """Détecte les mots-clés de durabilité dans un texte"""
    if pd.isna(text):
        return {'has_sustainability': False, 'keywords_found': [], 'count': 0}
    
    text = str(text).lower()
    found_keywords = []
    
    for keyword in keywords:
        if re.search(rf'\b{re.escape(keyword.lower())}\b', text):
            found_keywords.append(keyword)
    
    return {
        'has_sustainability': len(found_keywords) > 0,
        'keywords_found': found_keywords,
        'count': len(found_keywords)
    }

def run_ai_analysis(companies_path: Path) -> Path:
    """Exécute l'analyse IA sur les données entreprises"""
    logger = setup_module_logging()
    logger.info("Démarrage de l'analyse IA")
    
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Lecture des données
    companies = pd.read_csv(companies_path)
    
    # Mots-clés de durabilité
    sustainability_keywords = [
        'sustainable', 'sustainability', 'green', 'eco-friendly',
        'environmental', 'renewable', 'recycle', 'circular',
        'carbon', 'emission', 'ESG', 'ethical', 'organic',
        'fair trade', 'responsibility', 'clean', 'energy'
    ]
    
    # Analyse de chaque entreprise
    results = []
    
    for _, company in companies.iterrows():
        # Concaténation des champs textuels potentiels
        text_fields = []
        
        for field in ['company_name', 'original_name']:
            if field in company and pd.notna(company[field]):
                text_fields.append(str(company[field]))
        
        full_text = ' '.join(text_fields)
        
        # Détection des mots-clés
        detection = detect_sustainability_keywords(full_text, sustainability_keywords)
        
        result = {
            'company_id': company['company_id'],
            'company_name': company['company_name'],
            'has_sustainability': detection['has_sustainability'],
            'keywords_found': ', '.join(detection['keywords_found']),
            'keyword_count': detection['count']
        }
        
        results.append(result)
    
    # Création du DataFrame de résultats
    results_df = pd.DataFrame(results)
    
    # Statistiques
    sustainability_rate = results_df['has_sustainability'].mean() * 100
    logger.info(f"Taux de durabilité détecté: {sustainability_rate:.2f}%")
    
    # Sauvegarde
    output_path = OUTPUTS_DIR / "sustainability_analysis.csv"
    results_df.to_csv(output_path, index=False)
    
    logger.info(f"Analyse IA sauvegardée: {output_path}")
    
    return output_path