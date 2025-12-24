"""
Main orchestration script for OAR Data Pipeline
"""
import logging
import sys
from datetime import datetime
from pathlib import Path

# Import des modules du pipeline
from scrape_oar import download_oar_data
from clean_companies import clean_companies
from clean_facilities import process_facilities
from relational_builder import build_relational_tables
from analytics_dashboards import generate_analytics
from ai_module import run_ai_analysis
from export_final import export_final_results

# Configuration du logging
def setup_logging():
    """Configure le système de logging"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"pipeline_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def main():
    """Exécute le pipeline complet"""
    logger = setup_logging()
    logger.info("Démarrage du pipeline OAR")
    
    try:
        # Phase 1: Extraction des données
        logger.info("Phase 1: Extraction des données")
        raw_data_path = download_oar_data()
        
        # Phase 2: Nettoyage des entreprises
        logger.info("Phase 2: Nettoyage des entreprises")
        cleaned_companies_path = clean_companies(raw_data_path)
        
        # Phase 3: Traitement des établissements
        logger.info("Phase 3: Traitement des établissements")
        facilities_paths = process_facilities(cleaned_companies_path)
        
        # Phase 4: Structuration relationnelle
        logger.info("Phase 4: Structuration relationnelle")
        relational_paths = build_relational_tables(
            facilities_paths['companies'],
            facilities_paths['facilities'],
            facilities_paths['links']
        )
        
        # Phase 5: Analytics
        logger.info("Phase 5: Génération des tableaux de bord")
        analytics_paths = generate_analytics(relational_paths)
        
        # Phase 6: Module AI
        logger.info("Phase 6: Analyse IA")
        ai_results_path = run_ai_analysis(relational_paths['companies'])
        
        # Phase 7: Export final
        logger.info("Phase 7: Export final")
        final_report_path = export_final_results(
            relational_paths,
            analytics_paths,
            ai_results_path
        )
        
        logger.info(f"Pipeline terminé avec succès. Rapport: {final_report_path}")
        
    except Exception as e:
        logger.error(f"Erreur dans le pipeline: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()