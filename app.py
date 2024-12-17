# interface.py

import streamlit as st
import pandas as pd
from scraper import scrape_player_stats, select_all_nhl_matches_and_extract_data, fusionner_donnees_par_prenom_nom
from firebase_utils import initialize_firebase
from datetime import datetime, timedelta

# Initialize Firebase
try:
    db = initialize_firebase()
except Exception as e:
    st.error(f"Erreur d'initialisation de Firebase: {str(e)}")
    st.stop()

# Streamlit interface
st.title("Scraping des Statistiques des Joueurs de Hockey et des Cotes des Matchs")

# Cache management
def should_refresh_cache(last_update_time):
    if last_update_time is None:
        return True
    return datetime.now() - last_update_time > timedelta(minutes=5)

def load_data_from_firestore(collection_name, expected_columns=None):
    if not db:
        return None
        
    if collection_name not in st.session_state:
        st.session_state[collection_name] = {'data': None, 'last_update': None}
    
    if not should_refresh_cache(st.session_state[collection_name]['last_update']):
        return st.session_state[collection_name]['data']
    
    try:
        docs = db.collection(collection_name).stream()
        df = pd.DataFrame([doc.to_dict() for doc in docs]) if docs else None
        
        if df is not None and not df.empty and expected_columns:
            df = df.reindex(columns=expected_columns)
        
        st.session_state[collection_name]['data'] = df
        st.session_state[collection_name]['last_update'] = datetime.now()
        
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement des données depuis Firebase: {str(e)}")
        return None

# Load data when needed based on the selected menu
menu = st.sidebar.radio("Navigation", ("Stats joueurs", "Cote joueurs", "Stats + Cotes", "Tous les joueurs"))

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

elif menu == "Tous les joueurs":
    st.header("Tous les joueurs")
    
    # Bouton d'actualisation avec spinner
    if st.button("🔄 Actualiser avec les dernières données", key="all_players_refresh"):
        with st.spinner('Récupération des données...'):
            # Forcer le rafraîchissement des données
            if 'stats_joueurs' in st.session_state:
                del st.session_state['stats_joueurs']
            if 'cotes_joueurs' in st.session_state:
                del st.session_state['cotes_joueurs']
            # Réinitialiser les sélections
            st.session_state.selected_teams = []
            st.session_state.selected_positions = []
    
    # Chargement des données
    stats_columns = ["Prénom", "Nom", "Team", "Pos", "GP", "G", "A", "SOG", "SPCT", "TSA", "ATOI"]
    odds_columns = ["Prénom", "Nom", "Team", "Cote"]
    
    stats_df = load_data_from_firestore('stats_joueurs_database', stats_columns)
    odds_df = load_data_from_firestore('cotes_joueurs_database', odds_columns)
    
    if stats_df is not None and odds_df is not None:
        # Fusionner les données
        merged_df = fusionner_donnees_par_prenom_nom(stats_df, odds_df)
        
        if merged_df is not None:
            # Convertir les colonnes en numérique
            merged_df["Cote"] = pd.to_numeric(merged_df["Cote"], errors='coerce')
            merged_df["G"] = pd.to_numeric(merged_df["G"], errors='coerce')
            
            # Remplacer les NaN par des valeurs par défaut
            merged_df["Cote"] = merged_df["Cote"].fillna(999)
            merged_df["G"] = merged_df["G"].fillna(0)
            
            # Obtenir toutes les équipes et positions valides depuis les données originales
            all_valid_teams = sorted([str(team) for team in merged_df["Team"].unique() 
                                    if str(team) not in ["nan", "None", "", "Non assigné", "0"]])
            all_valid_positions = sorted([str(pos) for pos in merged_df["Pos"].unique() 
                                       if str(pos) not in ["nan", "None", "", "Non assigné"]])
            
            # Créer une copie pour les filtres
            filtered_df = merged_df.copy()
            
            # Filtres numériques
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Filtre de cote minimum
                max_cote = min(filtered_df[filtered_df["Cote"] < 999]["Cote"].max(), 10.0)
                min_cote = st.number_input("Cote minimum", 
                                         min_value=1.0,
                                         max_value=float(max_cote),
                                         value=1.0,
                                         step=0.1,
                                         key="all_players_min_cote")
            
            with col2:
                # Filtre de buts minimum
                max_buts = int(filtered_df["G"].max())
                min_buts = st.number_input("Nombre minimum de buts", 
                                         min_value=0,
                                         max_value=max_buts,
                                         value=0,
                                         key="all_players_min_buts")
            
            with col3:
                # Filtre pour exclure les cotes manquantes
                show_missing_odds = st.checkbox("Afficher les joueurs sans cote", 
                                             value=False,
                                             key="all_players_show_missing")
            
            # Expander pour les filtres d'équipe
            with st.expander("🏒 Filtrer par équipe"):
                # Boutons pour tout sélectionner/désélectionner
                col1_1, col1_2 = st.columns(2)
                
                # Initialiser la session state pour les équipes si nécessaire
                if "selected_teams" not in st.session_state:
                    st.session_state.selected_teams = all_valid_teams.copy()
                
                with col1_1:
                    if st.button("Tout sélectionner", key="select_all_teams"):
                        st.session_state.selected_teams = all_valid_teams.copy()
                with col1_2:
                    if st.button("Tout désélectionner", key="deselect_all_teams"):
                        st.session_state.selected_teams = []
                
                # Multiselect pour les équipes
                selected_teams = st.multiselect(
                    "Sélectionner les équipes",
                    options=all_valid_teams,
                    default=st.session_state.selected_teams,
                    key="teams_multiselect"
                )
                
                # Mettre à jour la session state uniquement si la sélection a changé
                if selected_teams != st.session_state.selected_teams:
                    st.session_state.selected_teams = selected_teams.copy()
            
            # Expander pour les filtres de position
            with st.expander("👥 Filtrer par position"):
                # Boutons pour tout sélectionner/désélectionner
                col2_1, col2_2 = st.columns(2)
                
                # Initialiser la session state pour les positions si nécessaire
                if "selected_positions" not in st.session_state:
                    st.session_state.selected_positions = all_valid_positions.copy()
                
                with col2_1:
                    if st.button("Tout sélectionner", key="select_all_positions"):
                        st.session_state.selected_positions = all_valid_positions.copy()
                with col2_2:
                    if st.button("Tout désélectionner", key="deselect_all_positions"):
                        st.session_state.selected_positions = []
                
                # Multiselect pour les positions
                selected_positions = st.multiselect(
                    "Sélectionner les positions",
                    options=all_valid_positions,
                    default=st.session_state.selected_positions,
                    key="positions_multiselect"
                )
                
                # Mettre à jour la session state uniquement si la sélection a changé
                if selected_positions != st.session_state.selected_positions:
                    st.session_state.selected_positions = selected_positions.copy()
            
            # Appliquer tous les filtres
            if not show_missing_odds:
                filtered_df = filtered_df[filtered_df["Cote"] < 999]
            
            filtered_df = filtered_df[
                (filtered_df["Cote"] >= min_cote) & 
                (filtered_df["G"] >= min_buts)
            ]
            
            # Appliquer les filtres de sélection
            if st.session_state.selected_teams:
                filtered_df = filtered_df[filtered_df["Team"].isin(st.session_state.selected_teams)]
            
            if st.session_state.selected_positions:
                filtered_df = filtered_df[filtered_df["Pos"].isin(st.session_state.selected_positions)]
            
            # Afficher le nombre total de joueurs
            st.write(f"Nombre total de joueurs : {len(filtered_df)}")
            
            # Afficher les données
            if not filtered_df.empty:
                st.dataframe(filtered_df[["Prénom", "Nom", "Team", "Pos", "GP", "G", "A", "SOG", "SPCT", "TSA", "ATOI", "Cote"]], 
                           use_container_width=True)
            else:
                st.warning("Aucun joueur ne correspond aux critères sélectionnés")
        else:
            st.error("Erreur lors de la fusion des données")
    else:
        st.error("Erreur lors du chargement des données")

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
