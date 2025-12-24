"""
Module d'export final et de génération de rapports
"""
import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict

FINAL_DIR = Path("data/final_export")

def setup_module_logging():
    return logging.getLogger(__name__)

def export_final_results(relational_paths: Dict[str, Path],
                         analytics_paths: Dict[str, Path],
                         ai_results_path: Path) -> Path:
    """Exporte les résultats finaux et génère un rapport"""
    logger = setup_module_logging()
    logger.info("Export final des résultats")
    
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Export des données relationnelles
    companies = pd.read_csv(relational_paths['companies'])
    facilities = pd.read_csv(relational_paths['facilities'])
    links = pd.read_csv(relational_paths['links'])
    
    # Fichier combiné
    combined_data = {
        'metadata': {
            'export_date': datetime.now().isoformat(),
            'total_companies': len(companies),
            'total_facilities': len(facilities),
            'total_links': len(links)
        },
        'companies': companies.to_dict('records'),
        'facilities': facilities.to_dict('records'),
        'links': links.to_dict('records')
    }
    
    combined_path = FINAL_DIR / f"oar_combined_{timestamp}.json"
    with open(combined_path, 'w', encoding='utf-8') as f:
        json.dump(combined_data, f, indent=2, ensure_ascii=False)
    
    # 2. Statistiques détaillées
    facilities_per_company = links.groupby('company_id').size()
    
    stats = {
        'summary': {
            'total_companies': len(companies),
            'total_facilities': len(facilities),
            'companies_with_facilities': facilities_per_company.shape[0],
            'avg_facilities_per_company': facilities_per_company.mean(),
            'median_facilities_per_company': facilities_per_company.median(),
            'max_facilities_per_company': facilities_per_company.max()
        },
        'companies_by_country': companies['country'].value_counts().to_dict(),
        'facilities_by_country': facilities['country'].value_counts().to_dict(),
        'export_timestamp': timestamp
    }
    
    stats_path = FINAL_DIR / f"summary_statistics_{timestamp}.json"
    with open(stats_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)
    
    # 3. Rapport texte
    report_path = FINAL_DIR / f"pipeline_report_{timestamp}.txt"
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("OAR DATA PIPELINE - RAPPORT FINAL\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"Date d'export: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("1. STATISTIQUES GLOBALES\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total entreprises: {stats['summary']['total_companies']}\n")
        f.write(f"Total établissements: {stats['summary']['total_facilities']}\n")
        f.write(f"Entreprises avec établissements: {stats['summary']['companies_with_facilities']}\n")
        f.write(f"Moyenne établissements/entreprise: {stats['summary']['avg_facilities_per_company']:.2f}\n")
        f.write(f"Médiane établissements/entreprise: {stats['summary']['median_facilities_per_company']:.2f}\n\n")
        
        f.write("2. RÉPARTITION PAR PAYS (Entreprises)\n")
        f.write("-" * 40 + "\n")
        for country, count in stats['companies_by_country'].items():
            f.write(f"{country}: {count} entreprises\n")
        
        f.write("\n3. FICHIERS GÉNÉRÉS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Données combinées: {combined_path.name}\n")
        f.write(f"Statistiques: {stats_path.name}\n")
        f.write(f"Analyse IA: {ai_results_path.name}\n")
        
        # Ajout des chemins des graphiques
        if analytics_paths:
            f.write(f"Graphique entreprises: {analytics_paths.get('companies_chart', '').name}\n")
            f.write(f"Graphique établissements: {analytics_paths.get('facilities_chart', '').name}\n")
    
    logger.info(f"Export final terminé. Rapport: {report_path}")
    
    return report_path