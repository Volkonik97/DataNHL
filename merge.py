# merge.py

import pandas as pd
from data_processing import enlever_accents_avec_remplacement
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('merge.log'),
        logging.StreamHandler()
    ]
)

def fusionner_donnees_par_prenom_nom(stats_df, odds_df):
    """
    Fusionne les données de statistiques et de cotes des joueurs.
    Utilise une approche simple et directe pour préserver tous les joueurs.
    """
    # Copier les DataFrames pour éviter de modifier les originaux
    stats = stats_df.copy()
    odds = odds_df.copy()

    # Normaliser les noms
    stats['Nom'] = stats['Nom'].apply(enlever_accents_avec_remplacement).str.strip()
    stats['Prénom'] = stats['Prénom'].apply(enlever_accents_avec_remplacement).str.strip()
    odds['Nom'] = odds['Nom'].apply(enlever_accents_avec_remplacement).str.strip()
    odds['Prénom'] = odds['Prénom'].apply(enlever_accents_avec_remplacement).str.strip()

    # Créer un dictionnaire des cotes
    cotes = {}
    for _, row in odds.iterrows():
        key = (row['Prénom'].strip(), row['Nom'].strip(), row['Team'].strip())
        cotes[key] = row['Cote']

    # Fonction pour trouver la cote d'un joueur
    def get_cote(prenom, nom, team):
        key = (prenom.strip(), nom.strip(), team.strip())
        return cotes.get(key, "Non disponible")

    # Ajouter la colonne des cotes aux statistiques
    stats['Cote'] = [get_cote(p, n, t) for p, n, t in zip(stats['Prénom'], stats['Nom'], stats['Team'])]

    # Trier par équipe et nom
    stats = stats.sort_values(['Team', 'Nom'])

    # Afficher les résultats pour débogage
    print("\n=== RÉSULTAT FUSION ===")
    print(f"Nombre total de joueurs après fusion: {len(stats)}")
    johnston_result = stats[stats['Nom'] == 'Johnston']
    print("\nJoueurs Johnston après fusion:")
    print(johnston_result[['Prénom', 'Nom', 'Team', 'Cote']].to_string())

    return stats

def merge_data():
    """
    Fonction principale pour fusionner les données des joueurs avec leurs cotes.
    """
    try:
        logging.info("Début du processus de fusion des données")
        
        # Récupération des statistiques des joueurs
        logging.info("Récupération des statistiques des joueurs...")
        stats_df = pd.read_csv('stats.csv')
        logging.info(f"Nombre de joueurs dans les statistiques: {len(stats_df)}")
        
        # Récupération des cotes des joueurs
        logging.info("Récupération des cotes des joueurs...")
        odds_df = pd.read_csv('odds.csv')
        logging.info(f"Nombre de joueurs dans les cotes: {len(odds_df)}")
        
        # Fusion des données
        logging.info("Fusion des données en cours...")
        merged_df = fusionner_donnees_par_prenom_nom(stats_df, odds_df)
        logging.info(f"Nombre de joueurs après fusion: {len(merged_df)}")
        
        # Vérification des données manquantes
        missing_odds = merged_df[merged_df['Cote'] == "Non disponible"].shape[0]
        logging.info(f"Nombre de joueurs sans cote: {missing_odds}")
        
        return merged_df
        
    except Exception as e:
        logging.error(f"Erreur lors de la fusion des données: {str(e)}")
        raise e

if __name__ == "__main__":
    try:
        merged_data = merge_data()
        print("Fusion des données terminée avec succès")
    except Exception as e:
        print(f"Erreur lors de l'exécution: {str(e)}")
