import streamlit as st
from firebase_admin import credentials, firestore
import firebase_admin
import tempfile
import json
import pandas as pd
import toml
import os

def initialize_firebase():
    # Vérifier si Firebase est déjà initialisé
    if not firebase_admin._apps:
        try:
            # Essayer d'abord de charger depuis un fichier local
            try:
                with open('.streamlit/secrets.toml', 'r') as f:
                    config = toml.load(f)
                    firebase_secrets = config['firebase_credentials']
            except Exception as e:
                st.error(f"Erreur lors de la lecture du fichier secrets.toml local: {str(e)}")
                # Si la lecture locale échoue, essayer via st.secrets
                firebase_secrets = st.secrets["firebase_credentials"]

            # Vérifier que tous les champs requis sont présents
            required_fields = [
                "type", "project_id", "private_key_id", "private_key",
                "client_email", "client_id", "auth_uri", "token_uri",
                "auth_provider_x509_cert_url", "client_x509_cert_url"
            ]
            
            for field in required_fields:
                if field not in firebase_secrets:
                    raise KeyError(f"Champ requis manquant dans les secrets Firebase: {field}")
            
            # Créer un fichier temporaire pour stocker les credentials
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                json.dump(firebase_secrets, temp_file)
                temp_file_path = temp_file.name
            
            # Initialiser Firebase avec le fichier de credentials temporaire
            cred = credentials.Certificate(temp_file_path)
            firebase_admin.initialize_app(cred)
            
            # Supprimer le fichier temporaire après utilisation
            os.unlink(temp_file_path)
            
            return firestore.client()
            
        except Exception as e:
            st.error(f"Erreur lors de l'initialisation de Firebase: {str(e)}")
            st.error("Vérifiez que le fichier .streamlit/secrets.toml existe et contient les bonnes informations")
            return None
    else:
        return firestore.client()

def update_firestore(collection_name, df):
    try:
        # Récupérer la collection Firestore
        db = initialize_firebase()
        collection_ref = db.collection(collection_name)
        
        # Supprimer tous les documents existants dans la collection
        batch_size = 500
        docs = collection_ref.limit(batch_size).stream()
        deleted = 0
        
        # Supprimer par lots pour éviter les timeouts
        while docs:
            batch = db.batch()
            for doc in docs:
                batch.delete(doc.reference)
                deleted += 1
            batch.commit()
            docs = collection_ref.limit(batch_size).stream()
        
        print(f"Suppression de {deleted} documents existants dans {collection_name}")
        
        # Ajouter les nouvelles données par lots
        batch = db.batch()
        added = 0
        for index, row in df.iterrows():
            doc_data = {k: None if pd.isna(v) else v for k, v in row.to_dict().items()}
            key = f"{doc_data['Prénom']}_{doc_data['Nom']}"
            doc_ref = collection_ref.document(key)
            batch.set(doc_ref, doc_data)
            added += 1
            
            # Commit le batch tous les 500 documents
            if added % 500 == 0:
                batch.commit()
                batch = db.batch()
        
        # Commit les documents restants
        if added % 500 != 0:
            batch.commit()
        
        print(f"Ajout de {added} nouveaux documents dans {collection_name}")
        return True
        
    except Exception as e:
        print(f"Erreur lors de la mise à jour de Firestore pour {collection_name}: {str(e)}")
        return False
