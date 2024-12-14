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
        
        cred_dict = dict(st.secrets["firebase_credentials"])
        
        # Ensure type field is correctly set
        if "type" not in cred_dict or cred_dict["type"] != "service_account":
            cred_dict["type"] = "service_account"
        
        # Vérifier que tous les champs requis sont présents
        missing_fields = [field for field in required_fields if field not in cred_dict]
        if missing_fields:
            raise ValueError(f"Missing required fields in credentials: {', '.join(missing_fields)}")
        
        # S'assurer que la private_key est correctement formatée
        if "private_key" in cred_dict:
            # Remove any existing BEGIN/END markers and whitespace
            key = cred_dict["private_key"].replace("-----BEGIN PRIVATE KEY-----", "")
            key = key.replace("-----END PRIVATE KEY-----", "")
            key = key.replace(" ", "")
            key = key.replace("\n", "")
            
            # Format the key properly with markers and newlines
            cred_dict["private_key"] = f"-----BEGIN PRIVATE KEY-----\n{key}\n-----END PRIVATE KEY-----\n"
        
        # Créer un fichier temporaire contenant les credentials
        with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".json") as temp_cred_file:
            json.dump(cred_dict, temp_cred_file)
            temp_cred_path = temp_cred_file.name

        # Charger les credentials depuis le fichier temporaire
        cred = credentials.Certificate(temp_cred_path)
        
        # Initialiser Firebase
        firebase_admin.initialize_app(cred)

    # Retourner le client Firestore
    return firestore.client()

def update_firestore_collection(collection_name, df):
    """
    Met à jour une collection Firestore avec les données d'un DataFrame de manière optimisée
    """
    if not firebase_admin._apps:
        initialize_firebase()
    
    if df is None or df.empty:
        print(f"DataFrame vide ou None pour {collection_name}")
        return False
    
    try:
        db = firestore.client()
        collection_ref = db.collection(collection_name)
        
        # Vérifier que les colonnes requises sont présentes
        required_columns = ['Prénom', 'Nom']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Colonnes manquantes dans le DataFrame: {missing_columns}")
            return False
        
        # Créer un index des documents existants par nom complet
        existing_docs = {}
        try:
            docs = collection_ref.stream()
            for doc in docs:
                data = doc.to_dict()
                if all(key in data for key in ['Prénom', 'Nom']):
                    key = f"{data['Prénom']}_{data['Nom']}"
                    existing_docs[key] = doc.reference
        except Exception as e:
            print(f"Erreur lors de la lecture des documents existants: {str(e)}")
            return False
        
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
