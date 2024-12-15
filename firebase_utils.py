import streamlit as st
from firebase_admin import credentials, firestore
import firebase_admin
import tempfile
import json
import pandas as pd

def initialize_firebase():
    # Vérifier si Firebase est déjà initialisé
    if not firebase_admin._apps:
        # Charger les credentials depuis les secrets et s'assurer que tous les champs requis sont présents
        required_fields = [
            "type", "project_id", "private_key_id", "private_key",
            "client_email", "client_id", "auth_uri", "token_uri",
            "auth_provider_x509_cert_url", "client_x509_cert_url"
        ]
        
        try:
            firebase_secrets = st.secrets["firebase"]
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
            
            # Retourner l'instance Firestore
            return firestore.client()
            
        except Exception as e:
            raise Exception(f"Erreur lors de l'initialisation de Firebase: {str(e)}")
    
    # Si Firebase est déjà initialisé, retourner simplement l'instance Firestore
    return firestore.client()

def update_firestore(collection_name, df):
    try:
        # Récupérer la collection Firestore
        collection_ref = initialize_firebase().collection(collection_name)
        
        # Récupérer les documents existants dans la collection
        existing_docs = {doc.id: doc for doc in collection_ref.stream()}
        
        # Mettre à jour ou ajouter les nouvelles données
        updates_count = 0
        for _, row in df.iterrows():
            try:
                doc_data = {k: None if pd.isna(v) else v for k, v in row.to_dict().items()}
                key = f"{doc_data['Prénom']}_{doc_data['Nom']}"
                
                if key in existing_docs:
                    # Mettre à jour le document existant
                    existing_docs[key].update(doc_data)
                else:
                    # Créer un nouveau document
                    collection_ref.add(doc_data)
                updates_count += 1
            except Exception as e:
                print(f"Erreur lors de la mise à jour/création du document pour {key}: {str(e)}")
                continue
        
        # Supprimer les documents qui n'existent plus dans le DataFrame
        deletes_count = 0
        for key, doc_ref in existing_docs.items():
            try:
                if key not in [f"{row['Prénom']}_{row['Nom']}" for _, row in df.iterrows()]:
                    doc_ref.delete()
                    deletes_count += 1
            except Exception as e:
                print(f"Erreur lors de la suppression du document {key}: {str(e)}")
                continue
        
        print(f"Mise à jour réussie pour {collection_name}:")
        print(f"- {updates_count} documents mis à jour/créés")
        print(f"- {deletes_count} documents supprimés")
        return True
        
    except Exception as e:
        print(f"Erreur lors de la mise à jour de Firestore pour {collection_name}: {str(e)}")
        return False
