"""
Module de génération des tableaux de bord analytiques
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from pathlib import Path
from typing import Dict

OUTPUTS_DIR = Path("data/outputs")

def setup_module_logging():
    return logging.getLogger(__name__)

def generate_analytics(relational_paths: Dict[str, Path]) -> Dict[str, Path]:
    """Génère les visualisations analytiques"""
    logger = setup_module_logging()
    logger.info("Génération des tableaux de bord")
    
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Lecture des données
    companies = pd.read_csv(relational_paths['companies'])
    links = pd.read_csv(relational_paths['links'])
    
    # 1. Nombre d'entreprises par pays
    logger.info("Création du graphique: Entreprises par pays")
    companies_by_country = companies['country'].value_counts().reset_index()
    companies_by_country.columns = ['country', 'count']
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=companies_by_country, x='country', y='count', palette='viridis')
    plt.title('Nombre d\'entreprises par pays', fontsize=16)
    plt.xlabel('Pays', fontsize=12)
    plt.ylabel('Nombre d\'entreprises', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    companies_chart_path = OUTPUTS_DIR / "companies_by_country.png"
    plt.savefig(companies_chart_path, dpi=300)
    plt.close()
    
    # 2. Nombre d'établissements par entreprise
    logger.info("Création du graphique: Établissements par entreprise")
    facilities_per_company = links.groupby('company_id').size().reset_index(name='facility_count')
    
    plt.figure(figsize=(12, 6))
    
    # Histogramme
    plt.subplot(1, 2, 1)
    plt.hist(facilities_per_company['facility_count'], bins=30, edgecolor='black', alpha=0.7)
    plt.title('Distribution des établissements par entreprise', fontsize=14)
    plt.xlabel('Nombre d\'établissements', fontsize=12)
    plt.ylabel('Nombre d\'entreprises', fontsize=12)
    
    # Box plot
    plt.subplot(1, 2, 2)
    plt.boxplot(facilities_per_company['facility_count'], vert=False)
    plt.title('Box Plot - Établissements par entreprise', fontsize=14)
    plt.xlabel('Nombre d\'établissements', fontsize=12)
    
    plt.tight_layout()
    facilities_chart_path = OUTPUTS_DIR / "facilities_per_company.png"
    plt.savefig(facilities_chart_path, dpi=300)
    plt.close()
    
    # 3. Statistiques supplémentaires
    stats = {
        'total_companies': len(companies),
        'total_facilities': len(pd.read_csv(relational_paths['facilities'])),
        'avg_facilities_per_company': facilities_per_company['facility_count'].mean(),
        'median_facilities_per_company': facilities_per_company['facility_count'].median(),
        'max_facilities_per_company': facilities_per_company['facility_count'].max()
    }
    
    stats_df = pd.DataFrame([stats])
    stats_path = OUTPUTS_DIR / "analytics_statistics.csv"
    stats_df.to_csv(stats_path, index=False)
    
    logger.info(f"Tableaux de bord sauvegardés dans: {OUTPUTS_DIR}")
    
    return {
        'companies_chart': companies_chart_path,
        'facilities_chart': facilities_chart_path,
        'statistics': stats_path
    }