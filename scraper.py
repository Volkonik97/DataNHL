# scraper.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import re
from data_processing import enlever_accents_avec_remplacement
import streamlit as st

# Dictionnaire de correspondance des noms d'équipes
TEAM_MAPPING = {
    'Anaheim Ducks': 'ANA',
    'Arizona Coyotes': 'ARI',
    'Boston Bruins': 'BOS',
    'Buffalo Sabres': 'BUF',
    'Calgary Flames': 'CGY',
    'Carolina Hurricanes': 'CAR',
    'Chicago Blackhawks': 'CHI',
    'Colorado Avalanche': 'COL',
    'Columbus Blue Jackets': 'CBJ',
    'Dallas Stars': 'DAL',
    'Detroit Red Wings': 'DET',
    'Edmonton Oilers': 'EDM',
    'Florida Panthers': 'FLA',
    'Los Angeles Kings': 'LAK',
    'Minnesota Wild': 'MIN',
    'Montreal Canadiens': 'MTL',
    'Nashville Predators': 'NSH',
    'New Jersey Devils': 'NJD',
    'New York Islanders': 'NYI',
    'New York Rangers': 'NYR',
    'Ottawa Senators': 'OTT',
    'Philadelphia Flyers': 'PHI',
    'Pittsburgh Penguins': 'PIT',
    'San Jose Sharks': 'SJS',
    'Seattle Kraken': 'SEA',
    'St. Louis Blues': 'STL',
    'Tampa Bay Lightning': 'TBL',
    'Toronto Maple Leafs': 'TOR',
    'Utah Hockey Club': 'UTH',
    'Vancouver Canucks': 'VAN',
    'Vegas Golden Knights': 'VGK',
    'Washington Capitals': 'WSH',
    'Winnipeg Jets': 'WPG'
}

# Dictionnaire des variations de prénoms
PRENOM_VARIATIONS = {
    'Alex': ['Alexander', 'Alexandre'],
    'Artemi': ['Artemy'],
    'Chris': ['Christopher'],
    'Matt': ['Matthew'],
    'Mike': ['Michael'],
    'Nick': ['Nicholas'],
    'Sam': ['Samuel'],
    'Tony': ['Anthony'],
    'Will': ['William'],
    'Bob': ['Robert'],
    'Dan': ['Daniel'],
    'Dave': ['David'],
    'Jim': ['James'],
    'Joe': ['Joseph'],
    'Tom': ['Thomas'],
    'Ben': ['Benjamin'],
    'Josh': ['Joshua'],
    'Tim': ['Timothy'],
    'Steve': ['Steven'],
    'Rob': ['Robert'],
    'Rick': ['Richard'],
    'Ondřej':['Ondrej'],
    'Mitchell':['Mitch'],
    'Evgeny':['Evgenii'],
    'Oscar':['Oskar'],
    'Pat':['Patrick'],
    'Tomáš':['Tomas'],
    'Dmitri':['Dmitry'],
    'Dmitry':['Dmitry'],
    'Evgenii':['Evgenii'],
    'Evgeny':['Evgenii'],
    'Anze':['Anže'],
    'JJ':['John-Jason'],
    'André':['Andre'],
    'Matty':['Matthew'],
    'Vasily':['Vasili'],
    'Jim':['James'],
    'Zach':['Zachary'],
}

# Créer un dictionnaire inversé pour rechercher le prénom standard à partir d'une variation
PRENOM_STANDARD = {}
for standard, variations in PRENOM_VARIATIONS.items():
    for var in variations:
        PRENOM_STANDARD[var] = standard
    PRENOM_STANDARD[standard] = standard

def normaliser_prenom(prenom):
    """Normalise un prénom en utilisant sa forme standard si elle existe"""
    prenom = prenom.strip()
    return PRENOM_STANDARD.get(prenom, prenom)

def scrape_player_stats():
    url_start = "https://www.hockey-reference.com/leagues/NHL_2025_skaters.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    data = requests.get(url_start, headers=headers)
    soup = BeautifulSoup(data.text, "html.parser")
    soup.find('tr', class_="over_header").decompose()
    stats_table = soup.find(id="player_stats")

    stats_table2024 = pd.read_html(str(stats_table))[0]
    columns_to_keep = ["Player", "Team", "Pos", "GP", "G", "A", "SOG", "SPCT", "TSA", "ATOI"]
    stats_table2024_clean = stats_table2024[columns_to_keep]
    stats_table2024_clean = stats_table2024_clean[stats_table2024_clean['Team'].apply(lambda x: len(str(x)) >= 3)]

    stats_table2024_clean = stats_table2024_clean.fillna(0)
    stats_table2024_clean['Player'] = stats_table2024_clean['Player'].apply(lambda x: x.encode('latin1').decode('utf-8') if isinstance(x, str) else x)
    stats_table2024_clean[['Prénom', 'Nom']] = stats_table2024_clean['Player'].str.extract(r'([^\s]+)\s*(.*)', expand=True)
    stats_table2024_clean['Prénom'].fillna('Non disponible', inplace=True)
    stats_table2024_clean['Nom'].fillna('Non disponible', inplace=True)
    stats_table2024_clean.drop(columns=['Player'], inplace=True)
    stats_table2024_clean['Nom'] = stats_table2024_clean['Nom'].apply(enlever_accents_avec_remplacement)
    stats_table2024_clean = stats_table2024_clean[['Prénom', 'Nom', 'Team', 'Pos', 'GP', 'G', 'A', 'SOG', 'SPCT', 'TSA', 'ATOI']]
    return stats_table2024_clean


def select_all_nhl_matches_and_extract_data():
    """
    Sélectionne tous les matchs NHL et extrait les données
    """
    driver = None
    data = []
    try:
        # Utiliser les identifiants depuis les secrets Streamlit
        username = st.secrets["credentials"]["username"]
        password = st.secrets["credentials"]["password"]
        
        # Configuration de Chromium pour Streamlit Cloud
        chrome_options = Options()
        
        # Configuration pour le mode headless
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        
        # Configuration spécifique pour Chromium sur Debian
        chrome_options.binary_location = "/usr/bin/chromium"
        
        # Options supplémentaires
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # User agent
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        st.write("Configuration du navigateur Chromium en mode headless...")
        
        # Utilisation du ChromeDriver installé via packages.txt
        service = Service('/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        st.write("Navigateur Chromium démarré avec succès!")
        driver.set_page_load_timeout(30)
        
        # Vérification du mode headless
        st.write(f"Mode headless actif: {driver.execute_script('return navigator.webdriver')}")
        
        login_url = "https://maxicotes.fr/wp-login.php"
        driver.get(login_url)

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "user_login"))
        )

        username_input = driver.find_element(By.ID, "user_login")
        username_input.send_keys(username)

        password_input = driver.find_element(By.ID, "user_pass")
        password_input.send_keys(password)

        login_button = driver.find_element(By.ID, "wp-submit")
        login_button.click()

        try:
            popup_close_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.pum-close.popmake-close"))
            )
            popup_close_button.click()
        except:
            print("No popup found or failed to close.")

        url = "https://maxicotes.fr/hockey-buteur"
        driver.get(url)
        time.sleep(2)

        try:
            dropdown_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'accordion-btn') and contains(text(), 'NHL')]"))
            )
            driver.execute_script("arguments[0].scrollIntoView();", dropdown_button)
            time.sleep(2)
            dropdown_button.click()

            bloc_nhl = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, "//div[@class='panel' and preceding-sibling::button[contains(text(), 'NHL')]]"))
            )
        except Exception as e:
            print(f"Error opening NHL dropdown block: {e}")

        try:
            all_match_radios = driver.find_elements(By.XPATH, "//div[@class='panel' and preceding-sibling::button[contains(text(), 'NHL')]]//input[@type='radio'][@name='match']")
            total_matches = len(all_match_radios)

            for i in range(total_matches):
                all_match_radios = driver.find_elements(By.XPATH, "//div[@class='panel' and preceding-sibling::button[contains(text(), 'NHL')]]//input[@type='radio'][@name='match']")
                if i < len(all_match_radios):
                    match_radio = all_match_radios[i]
                    driver.execute_script("arguments[0].scrollIntoView();", match_radio)
                    WebDriverWait(driver, 5).until(EC.element_to_be_clickable(match_radio)).click()
                    time.sleep(1)

                    try:
                        rows = driver.find_elements(By.CSS_SELECTOR, "table.result-table tbody tr")
                        for row in rows:
                            player_cell = row.find_element(By.CSS_SELECTOR, "td")
                            full_text = player_cell.text
                            # Extraire le nom du joueur et l'équipe
                            match = re.match(r"(.*?)\s*\((.*?)\)", full_text)
                            if match:
                                player_name = match.group(1).strip()
                                team_name = match.group(2).strip()
                                # Convertir le nom complet de l'équipe en abréviation
                                team_abbrev = TEAM_MAPPING.get(team_name, "")
                            else:
                                player_name = re.sub(r"\s*\(.*?\)", "", full_text).strip()
                                team_abbrev = ""

                            odds_elements = row.find_elements(By.CSS_SELECTOR, "td.center-cell .oval-background")
                            odds = {float(od.text) for od in odds_elements[1:] if od.text.strip()}
                            if odds:
                                highest_odd = max(odds)
                                data.append([player_name, team_abbrev, highest_odd])
                            else:
                                data.append([player_name, team_abbrev, "Pas de cote disponible"])
                    except Exception as e:
                        print(f"Error extracting table data: {e}")
        except Exception as e:
            print(f"Error selecting matches: {e}")

    except Exception as e:
        st.error(f"Une erreur s'est produite lors du scraping: {str(e)}")
        return pd.DataFrame()  # Retourner un DataFrame vide en cas d'erreur
    finally:
        if driver is not None:
            try:
                driver.quit()
                st.write("Navigateur fermé avec succès")
            except Exception:
                st.warning("Erreur lors de la fermeture du navigateur")

    # Si aucune donnée n'a été récupérée
    if not data:
        st.warning("Aucune cote n'a été trouvée pour les joueurs.")
        return pd.DataFrame()

    # Création du DataFrame avec les données récupérées
    df = pd.DataFrame(data, columns=["Player", "Team", "Cote"])
    df[['Prénom', 'Nom']] = df['Player'].str.extract(r'([^\s]+)\s*(.*)', expand=True)
    
    # Nettoyage des données
    df['Prénom'] = df['Prénom'].fillna('Non disponible')
    df['Nom'] = df['Nom'].fillna('Non disponible')
    
    # Réorganisation des colonnes
    df = df[['Prénom', 'Nom', 'Team', 'Cote']]
    
    # Mettre en cache les résultats
    if 'last_scraping_result' not in st.session_state:
        st.session_state.last_scraping_result = None
    st.session_state.last_scraping_result = df
    
    st.success(f"Scraping terminé avec succès! {len(df)} joueurs trouvés.")
    return df


def fusionner_donnees_par_prenom_nom(stats_df, odds_df):
    """
    Fusionne les données de statistiques et de cotes des joueurs.
    """
    # Faire une copie des DataFrames
    stats = stats_df.copy()
    odds = odds_df.copy()
    
    # Normaliser les noms et prénoms
    stats['Nom'] = stats['Nom'].apply(enlever_accents_avec_remplacement).str.strip()
    stats['Prénom'] = stats['Prénom'].apply(normaliser_prenom)
    odds['Nom'] = odds['Nom'].apply(enlever_accents_avec_remplacement).str.strip()
    odds['Prénom'] = odds['Prénom'].apply(normaliser_prenom)
    
    # Créer une clé unique pour chaque joueur
    stats['key'] = stats.apply(lambda x: f"{x['Prénom']}_{x['Nom']}", axis=1)
    odds['key'] = odds.apply(lambda x: f"{x['Prénom']}_{x['Nom']}", axis=1)
    
    # Créer un dictionnaire des cotes avec gestion des variations de prénoms
    cotes_dict = {}
    for _, row in odds.iterrows():
        key = f"{row['Prénom']}_{row['Nom']}"
        cotes_dict[key] = row['Cote']
        
        # Ajouter des entrées pour les variations de prénoms
        if row['Prénom'] in PRENOM_VARIATIONS:
            for variation in PRENOM_VARIATIONS[row['Prénom']]:
                var_key = f"{variation}_{row['Nom']}"
                cotes_dict[var_key] = row['Cote']
    
    # Appliquer les cotes
    def get_cote(row):
        key = f"{row['Prénom']}_{row['Nom']}"
        return cotes_dict.get(key, "Non disponible")
    
    # Ajouter les cotes
    stats['Cote'] = stats.apply(get_cote, axis=1)
    
    # Nettoyer et retourner le résultat
    stats = stats.drop('key', axis=1)
    
    # Trier seulement si la colonne Team existe
    if 'Team' in stats.columns:
        return stats.sort_values(['Team', 'Nom'])
    else:
        return stats.sort_values('Nom')
