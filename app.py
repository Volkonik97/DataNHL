# interface.py

import streamlit as st
import pandas as pd
from scraper import scrape_player_stats, select_all_nhl_matches_and_extract_data, fusionner_donnees_par_prenom_nom
from firebase_utils import initialize_firebase, update_firestore_collection
from datetime import datetime, timedelta

# Initialize Firebase
try:
    db = initialize_firebase()
    firebase_initialized = True
except Exception as e:
    st.error(f"Erreur lors de l'initialisation de Firebase: {str(e)}")
    firebase_initialized = False

# Streamlit interface
st.title("Scraping des Statistiques des Joueurs de Hockey et des Cotes des Matchs")

# Cache management
def should_refresh_cache(last_update_time):
    if last_update_time is None:
        return True
    return datetime.now() - last_update_time > timedelta(minutes=5)

def load_data_from_firestore(collection_name, expected_columns=None):
    if not firebase_initialized:
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
    
    # Utiliser les données déjà scrapées des autres onglets
    stats_df = st.session_state.get('stats')
    odds_df = st.session_state.get('odds_data')
    
    if stats_df is not None and odds_df is not None:
        if st.button("Fusionner et afficher les données", key="merge_data"):
            # Fusionner les données
            merged_data = fusionner_donnees_par_prenom_nom(stats_df, odds_df)
            st.session_state.merged_data = merged_data
            st.session_state.last_merge_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Sauvegarder les données fusionnées dans la session pour l'onglet "Tous les joueurs"
            st.session_state['latest_stats_df'] = stats_df
            st.session_state['latest_odds_df'] = odds_df
            
            # Afficher les données après fusion
            st.write("### Données après fusion:")
            st.write(f"Nombre total de joueurs: {len(merged_data)}")
            st.success("Données fusionnées avec succès!")
    else:
        st.warning("Veuillez d'abord scraper les données dans les onglets 'Stats joueurs' et 'Cotes joueurs'")
    
    # Afficher les données fusionnées par équipe
    if hasattr(st.session_state, 'merged_data') and st.session_state.merged_data is not None:
        # Filtrer pour exclure l'équipe "0" et convertir toutes les équipes en string avant le tri
        filtered_data = st.session_state.merged_data[st.session_state.merged_data['Team'].astype(str) != "0"]
        
        # Créer deux colonnes pour les champs de recherche
        search_col1, search_col2 = st.columns(2)
        
        with search_col1:
            # Champ de recherche pour les équipes
            search_team = st.text_input("Rechercher une équipe (ex: MTL, TOR, BOS...)", "").upper()
        
        with search_col2:
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
            
        teams = sorted(filtered_data['Team'].astype(str).unique())
        
        # Filtrer les équipes selon la recherche d'équipe
        if search_team:
            teams = [team for team in teams if search_team in team.upper()]
            if not teams:
                st.warning(f"Aucune équipe trouvée pour '{search_team}'")
        
        # Créer des onglets pour chaque équipe
        if teams:
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
    
    # Chargement des données
    stats_columns = ["Prénom", "Nom", "Team", "Pos", "GP", "G", "A", "SOG", "SPCT", "TSA", "ATOI"]
    odds_columns = ["Prénom", "Nom", "Team", "Cote"]
    
    stats_df = load_data_from_firestore('stats_joueurs_database', stats_columns)
    odds_df = load_data_from_firestore('cotes_joueurs_database', odds_columns)
    
    if stats_df is not None and odds_df is not None:
        # Fusionner les données
        merged_df = fusionner_donnees_par_prenom_nom(stats_df, odds_df)
        
        if merged_df is not None:
            # Filtres pour les données
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Filtre de cote minimum
                min_cote = st.number_input("Cote minimum", 
                                         min_value=1.0,
                                         max_value=float(merged_df["Cote"].max()),
                                         value=1.0,
                                         step=0.1)
            
            with col2:
                # Filtre de buts minimum
                min_buts = st.number_input("Nombre minimum de buts", 
                                         min_value=0,
                                         max_value=int(merged_df["G"].max()),
                                         value=0)
            
            with col3:
                # Filtre pour exclure les cotes manquantes
                show_missing_odds = st.checkbox("Afficher les joueurs sans cote", value=False)
            
            # Créer une copie pour les filtres
            filtered_df = merged_df.copy()
            
            # Appliquer les filtres
            if not show_missing_odds:
                filtered_df = filtered_df[filtered_df["Cote"] < 999]  # Exclure les joueurs sans cote
            
            filtered_df = filtered_df[
                (filtered_df["Cote"] >= min_cote) & 
                (filtered_df["G"] >= min_buts)
            ]
            
            # Expander pour les filtres d'équipe
            with st.expander(" Filtrer par équipe"):
                # Obtenir toutes les équipes valides
                valid_teams = sorted([str(team) for team in filtered_df["Team"].unique() 
                                   if str(team) not in ["nan", "None", "", "Non assigné", "0"]])
                
                # Boutons pour tout sélectionner/désélectionner
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Tout sélectionner", key="select_all_teams"):
                        st.session_state.selected_teams = valid_teams
                with col2:
                    if st.button("Tout désélectionner", key="deselect_all_teams"):
                        st.session_state.selected_teams = []
                
                # Initialiser la session state si nécessaire
                if 'selected_teams' not in st.session_state:
                    st.session_state.selected_teams = valid_teams
                
                # Créer les cases à cocher
                selected_teams = []
                for team in valid_teams:
                    if st.checkbox(team, value=team in st.session_state.selected_teams, key=f"team_{team}"):
                        selected_teams.append(team)
                st.session_state.selected_teams = selected_teams
            
            # Expander pour les filtres de position
            with st.expander(" Filtrer par position"):
                # Obtenir toutes les positions valides
                valid_positions = sorted([str(pos) for pos in filtered_df["Pos"].unique() 
                                       if str(pos) not in ["nan", "None", "", "Non assigné"]])
                
                # Boutons pour tout sélectionner/désélectionner
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Tout sélectionner", key="select_all_pos"):
                        st.session_state.selected_positions = valid_positions
                with col2:
                    if st.button("Tout désélectionner", key="deselect_all_pos"):
                        st.session_state.selected_positions = []
                
                # Initialiser la session state si nécessaire
                if 'selected_positions' not in st.session_state:
                    st.session_state.selected_positions = valid_positions
                
                # Créer les cases à cocher
                selected_positions = []
                for pos in valid_positions:
                    if st.checkbox(pos, value=pos in st.session_state.selected_positions, key=f"pos_{pos}"):
                        selected_positions.append(pos)
                st.session_state.selected_positions = selected_positions
            
            # Appliquer les filtres d'équipe et de position
            if selected_teams:
                filtered_df = filtered_df[filtered_df["Team"].isin(selected_teams)]
            if selected_positions:
                filtered_df = filtered_df[filtered_df["Pos"].isin(selected_positions)]
            
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
    
    # Bouton d'actualisation avec spinner
    if st.button(" Actualiser avec les dernières données"):
        with st.spinner("Mise à jour des données en cours..."):
            # Récupérer les dernières données fusionnées
            if 'merged_data' not in st.session_state:
                st.warning("Veuillez d'abord fusionner les données dans l'onglet Stats + Cotes")
                st.stop()
            
            merged_df = st.session_state.merged_data
            if merged_df is None:
                st.warning("Aucune donnée fusionnée disponible")
                st.stop()
            
            latest_stats = st.session_state.get('latest_stats_df')
            latest_odds = st.session_state.get('latest_odds_df')
            
            if latest_stats is None or latest_odds is None:
                st.warning("Données de stats ou de cotes manquantes")
                st.stop()
            
            # Mettre à jour Firebase avec les données fusionnées
            stats_success = update_firestore_collection('stats_joueurs_database', latest_stats)
            odds_success = update_firestore_collection('cotes_joueurs_database', latest_odds)
            
            if stats_success and odds_success:
                st.success("Données mises à jour avec succès!")
            else:
                error_msg = []
                if not stats_success:
                    error_msg.append("Erreur lors de la mise à jour des statistiques")
                if not odds_success:
                    error_msg.append("Erreur lors de la mise à jour des cotes")
                st.error("\n".join(error_msg))
    
    # Chargement des données
    stats_columns = ["Prénom", "Nom", "Team", "Pos", "GP", "G", "A", "SOG", "SPCT", "TSA", "ATOI"]
    odds_columns = ["Prénom", "Nom", "Team", "Cote"]
    
    stats_df = load_data_from_firestore('stats_joueurs_database', stats_columns)
    odds_df = load_data_from_firestore('cotes_joueurs_database', odds_columns)
    
    if stats_df is not None and odds_df is not None:
        # Conversion des colonnes en types numériques
        stats_df["G"] = pd.to_numeric(stats_df["G"], errors='coerce')
        
        # Conversion des cotes en nombres flottants
        odds_df["Cote"] = pd.to_numeric(odds_df["Cote"], errors='coerce')
        
        # Fusion des données
        merged_df = pd.merge(stats_df, odds_df[["Prénom", "Nom", "Cote"]], 
                           on=["Prénom", "Nom"], how="outer")
        
        # Remplacement des NaN par 0 pour les buts et 999 pour les cotes
        merged_df["G"] = merged_df["G"].fillna(0)
        merged_df["Cote"] = merged_df["Cote"].fillna(999)
        
        # Conversion de la colonne Team en chaînes et remplacement des valeurs manquantes
        merged_df["Team"] = merged_df["Team"].astype(str)
        merged_df.loc[merged_df["Team"].isin(["nan", "None", ""]), "Team"] = "Non assigné"
        
        # Conversion de la colonne Pos en chaînes et remplacement des valeurs manquantes
        merged_df["Pos"] = merged_df["Pos"].astype(str)
        merged_df.loc[merged_df["Pos"].isin(["nan", "None", ""]), "Pos"] = "Non assigné"
        
        # Filtres dans des colonnes
        col1, col2 = st.columns([2, 2])
        
        with col1:
            with st.expander(" Sélection des équipes"):
                # Liste des équipes avec cases à cocher
                valid_teams = sorted([str(team) for team in merged_df["Team"].unique() 
                                    if str(team) not in ["nan", "None", "", "Non assigné"]])
                
                # Boutons pour tout sélectionner/désélectionner
                col1_1, col1_2 = st.columns(2)
                with col1_1:
                    if st.button("Tout sélectionner", key="select_all_teams"):
                        st.session_state.selected_teams = valid_teams
                with col1_2:
                    if st.button("Tout désélectionner", key="deselect_all_teams"):
                        st.session_state.selected_teams = []
                
                # Initialiser la session state si nécessaire
                if 'selected_teams' not in st.session_state:
                    st.session_state.selected_teams = valid_teams
                
                # Créer les cases à cocher
                selected_teams = []
                for team in valid_teams:
                    if st.checkbox(team, value=team in st.session_state.selected_teams, key=f"team_{team}"):
                        selected_teams.append(team)
                st.session_state.selected_teams = selected_teams
            
        with col2:
            with st.expander(" Sélection des positions"):
                # Liste des positions avec cases à cocher
                valid_positions = sorted([str(pos) for pos in merged_df["Pos"].unique() 
                                       if str(pos) not in ["nan", "None", "", "Non assigné"]])
                
                # Boutons pour tout sélectionner/désélectionner
                col2_1, col2_2 = st.columns(2)
                with col2_1:
                    if st.button("Tout sélectionner", key="select_all_pos"):
                        st.session_state.selected_positions = valid_positions
                with col2_2:
                    if st.button("Tout désélectionner", key="deselect_all_pos"):
                        st.session_state.selected_positions = []
                
                # Initialiser la session state si nécessaire
                if 'selected_positions' not in st.session_state:
                    st.session_state.selected_positions = valid_positions
                
                # Créer les cases à cocher
                selected_positions = []
                for pos in valid_positions:
                    if st.checkbox(pos, value=pos in st.session_state.selected_positions, key=f"pos_{pos}"):
                        selected_positions.append(pos)
                st.session_state.selected_positions = selected_positions
        
        # Filtres numériques dans une nouvelle ligne
        col3, col4 = st.columns(2)
        with col3:
            min_cote = st.number_input("Cote minimum", 
                                     min_value=1.0,
                                     max_value=float(merged_df["Cote"].max()),
                                     value=1.0,
                                     step=0.1)
        with col4:
            min_buts = st.number_input("Nombre minimum de buts", 
                                     min_value=0,
                                     max_value=int(merged_df["G"].max()),
                                     value=0)
        
        # Application des filtres
        filtered_df = merged_df[
            (merged_df["Cote"] >= min_cote) & 
            (merged_df["G"] >= min_buts) &
            (merged_df["Cote"] < 999)  # Exclure les joueurs sans cote
        ]
        
        # Filtre par équipes sélectionnées
        if selected_teams:
            filtered_df = filtered_df[filtered_df["Team"].isin(selected_teams)]
            
        # Filtre par position(s)
        if selected_positions:
            filtered_df = filtered_df[filtered_df["Pos"].isin(selected_positions)]
        
        # Tri des colonnes et remplacement des valeurs manquantes
        display_columns = ["Prénom", "Nom", "Team", "Pos", "GP", "G", "A", "Cote"]
        filtered_df = filtered_df[display_columns].copy()
        
        # Remplir les valeurs manquantes pour l'affichage
        for col in display_columns:
            if col not in ["G", "Cote"]:  # Ne pas remplir les colonnes numériques
                filtered_df[col] = filtered_df[col].fillna("Non disponible")
        
        # Tri final
        filtered_df = filtered_df.sort_values(["Team", "Nom"])
        
        # Statistiques par équipe
        if len(filtered_df) > 0:
            st.write(f"Nombre de joueurs affichés : {len(filtered_df)}")
            
            # Statistiques agrégées par équipe
            team_stats = filtered_df.groupby("Team").agg({
                "G": "sum",
                "Cote": "mean"
            }).round(2)
            
            # Afficher les statistiques par équipe dans un expander
            with st.expander("Statistiques par équipe"):
                st.dataframe(team_stats, use_container_width=True)
            
            # Afficher le tableau principal
            st.dataframe(filtered_df, use_container_width=True)
        else:
            st.warning("Aucun joueur ne correspond aux critères sélectionnés.")
    else:
        st.warning("Veuillez d'abord récupérer les statistiques et les cotes des joueurs.")

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
