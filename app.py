# interface.py

import streamlit as st
import pandas as pd
from scraper import scrape_player_stats, select_all_nhl_matches_and_extract_data, fusionner_donnees_par_prenom_nom
from firebase_utils import initialize_firebase
from datetime import datetime, timedelta

# Initialize Firebase
db = initialize_firebase()

# Streamlit interface
st.title("Scraping des Statistiques des Joueurs de Hockey et des Cotes des Matchs")

# Cache management
def should_refresh_cache(last_update_time):
    if last_update_time is None:
        return True
    return datetime.now() - last_update_time > timedelta(minutes=5)

def load_data_from_firestore(collection_name, expected_columns=None):
    if collection_name not in st.session_state:
        st.session_state[collection_name] = {'data': None, 'last_update': None}
    
    if not should_refresh_cache(st.session_state[collection_name]['last_update']):
        return st.session_state[collection_name]['data']
    
    docs = db.collection(collection_name).stream()
    df = pd.DataFrame([doc.to_dict() for doc in docs]) if docs else None
    
    if df is not None and not df.empty and expected_columns:
        df = df.reindex(columns=expected_columns)
    
    st.session_state[collection_name]['data'] = df
    st.session_state[collection_name]['last_update'] = datetime.now()
    
    return df

# Load data when needed based on the selected menu
menu = st.sidebar.radio("Navigation", ("Stats joueurs", "Cote joueurs", "Stats + Cotes"))

if menu == "Stats joueurs":
    stats_columns = ["Prénom", "Nom", "Team", "Pos", "GP", "G", "A", "SOG", "SPCT", "TSA", "ATOI"]
    st.session_state.stats = load_data_from_firestore('stats_joueurs_database', stats_columns)
    if st.session_state.stats is not None:
        st.session_state.stats = st.session_state.stats.sort_values(['Team', 'Nom'])
    
    st.header("Statistiques des Joueurs")
    if 'last_scrape_time' in st.session_state:
        st.write(f"Dernière mise à jour : {st.session_state.last_scrape_time}")
    if st.button("Démarrer le scraping des statistiques des joueurs", key="scrape_stats", help="Cliquez pour démarrer le scraping des statistiques des joueurs"):
        with st.spinner('Récupération des statistiques des joueurs...'):
            st.session_state.last_scrape_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            st.session_state.stats = scrape_player_stats()
            # Stocker les statistiques dans Firebase avec un batch pour éviter le dépassement de quota
            stats_dict = st.session_state.stats.to_dict(orient='records')
            batch = db.batch()
            for player in stats_dict:
                # Créer une clé unique en combinant prénom et nom
                doc_id = f"{player['Prénom']}_{player['Nom']}".replace(" ", "_")
                doc_ref = db.collection('stats_joueurs_database').document(doc_id)
                batch.set(doc_ref, player)
            batch.commit()
        st.success("Statistiques récupérées et stockées avec succès!")
    if st.session_state.stats is not None:
        st.dataframe(st.session_state.stats, use_container_width=True)

elif menu == "Cote joueurs":
    odds_columns = ["Prénom", "Nom", "Cote"]
    st.session_state.odds_data = load_data_from_firestore('cotes_joueurs_database', odds_columns)
    if st.session_state.odds_data is not None:
        st.session_state.odds_data = st.session_state.odds_data.sort_values('Nom')
    
    st.header("Cotes des Joueurs")
    if 'last_odds_scrape_time' in st.session_state:
        st.write(f"Dernière mise à jour : {st.session_state.last_odds_scrape_time}")
    if st.button("Démarrer le scraping des cotes des matchs", key="scrape_odds", help="Cliquez pour démarrer le scraping des cotes des matchs"):
        with st.spinner('Récupération des cotes des matchs...'):
            st.session_state.last_odds_scrape_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            st.session_state.odds_data = select_all_nhl_matches_and_extract_data()
            if not st.session_state.odds_data.empty:
                # Stocker les cotes dans Firebase avec un batch pour éviter le dépassement de quota
                odds_dict = st.session_state.odds_data.to_dict(orient='records')
                batch = db.batch()
                for player in odds_dict:
                    # Créer une clé unique en combinant prénom et nom
                    doc_id = f"{player['Prénom']}_{player['Nom']}".replace(" ", "_")
                    doc_ref = db.collection('cotes_joueurs_database').document(doc_id)
                    batch.set(doc_ref, player)
                batch.commit()
                st.success("Cotes des matchs récupérées et stockées avec succès!")
            else:
                st.warning("Aucune cote n'a été trouvée pour les joueurs.")
    if st.session_state.odds_data is not None:
        st.dataframe(st.session_state.odds_data, use_container_width=True)

elif menu == "Stats + Cotes":
    st.header("Statistiques et Cotes des Joueurs")
    if 'last_merge_time' in st.session_state:
        st.write(f"Dernière fusion des données : {st.session_state.last_merge_time}")
    
    # Charger les données nécessaires si ce n'est pas déjà fait
    stats_columns = ["Prénom", "Nom", "Team", "Pos", "GP", "G", "A", "SOG", "SPCT", "TSA", "ATOI"]
    odds_columns = ["Prénom", "Nom", "Cote"]
    
    if 'stats' not in st.session_state or st.session_state.stats is None:
        st.session_state.stats = load_data_from_firestore('stats_joueurs_database', stats_columns)
    
    if 'odds_data' not in st.session_state or st.session_state.odds_data is None:
        st.session_state.odds_data = load_data_from_firestore('cotes_joueurs_database', odds_columns)
    
    if st.button("Fusionner les données et afficher", key="merge_data", help="Cliquez pour fusionner les données de statistiques et de cotes"):
        if st.session_state.stats is not None and st.session_state.odds_data is not None:
            # Fusionner les données
            merged_data = fusionner_donnees_par_prenom_nom(st.session_state.stats, st.session_state.odds_data)
            st.session_state.merged_data = merged_data
            
            # Afficher les données après fusion
            st.write("### Données après fusion:")
            st.write(f"Nombre total de joueurs: {len(merged_data)}")
            st.success("Données fusionnées avec succès!")

    # Afficher les données fusionnées par équipe
    if hasattr(st.session_state, 'merged_data') and st.session_state.merged_data is not None:
        # Filtrer pour exclure l'équipe "0" et convertir toutes les équipes en string avant le tri
        filtered_data = st.session_state.merged_data[st.session_state.merged_data['Team'].astype(str) != "0"]
        
        # Créer deux colonnes pour les champs de recherche
        col1, col2 = st.columns(2)
        
        with col1:
            # Champ de recherche pour les équipes
            search_team = st.text_input("Rechercher une équipe (ex: MTL, TOR, BOS...)", "").upper()
        
        with col2:
            # Champ de recherche pour les joueurs
            search_player = st.text_input("Rechercher un joueur (nom ou prénom)", "").strip()
        
        # Filtrer les données selon la recherche de joueur
        if search_player:
            search_terms = search_player.lower().split()
            mask = filtered_data.apply(lambda x: any(term in x['Nom'].lower() for term in search_terms) or 
                                               any(term in x['Prénom'].lower() for term in search_terms), axis=1)
            filtered_data = filtered_data[mask]
            if filtered_data.empty:
                st.warning(f"Aucun joueur trouvé pour '{search_player}'")
                st.stop()
            
        teams = sorted(filtered_data['Team'].astype(str).unique())
        
        # Filtrer les équipes selon la recherche d'équipe
        if search_team:
            teams = [team for team in teams if search_team in team.upper()]
            if not teams:
                st.warning(f"Aucune équipe trouvée pour '{search_team}'")
                st.stop()
        
        # Créer des onglets pour chaque équipe
        tabs = st.tabs(teams)
        
        # Pour chaque équipe, afficher ses joueurs dans son onglet
        for team, tab in zip(teams, tabs):
            with tab:
                team_data = filtered_data[filtered_data['Team'].astype(str) == team].copy()
                if not team_data.empty:
                    columns = ["Prénom", "Nom", "Pos", "GP", "G", "A", "SOG", "SPCT", "TSA", "ATOI", "Cote"]
                    st.dataframe(team_data[columns], use_container_width=True)

# Options d'exportation des données localement
if st.sidebar.button("Télécharger les données", key="download_data"):
    if st.session_state.stats is not None:
        st.download_button(
            label="Télécharger les Statistiques des Joueurs en CSV",
            data=st.session_state.stats.to_csv(index=False).encode('utf-8'),
            file_name='stats_joueurs.csv',
            mime='text/csv'
        )
    if st.session_state.odds_data is not None:
        st.download_button(
            label="Télécharger les Cotes des Matchs en CSV",
            data=st.session_state.odds_data.to_csv(index=False).encode('utf-8'),
            file_name='cotes_matchs.csv',
            mime='text/csv'
        )
    if st.session_state.merged_data is not None:
        st.download_button(
            label="Télécharger les Données Fusionnées en CSV",
            data=st.session_state.merged_data.to_csv(index=False).encode('utf-8'),
            file_name='donnees_fusionnees.csv',
            mime='text/csv'
        )
