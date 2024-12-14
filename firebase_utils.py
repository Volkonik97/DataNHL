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
        return False
    
    db = firestore.client()
    collection_ref = db.collection(collection_name)
    batch = db.batch()
    batch_size = 0
    max_batch_size = 500  # Limite de Firestore pour les opérations par batch
    
    try:
        # Créer un index des documents existants par nom complet
        existing_docs = {}
        docs = collection_ref.stream()
        for doc in docs:
            data = doc.to_dict()
            if 'Prénom' in data and 'Nom' in data:
                key = f"{data['Prénom']}_{data['Nom']}"
                existing_docs[key] = doc.reference
        
        # Mettre à jour ou ajouter les nouvelles données
        for _, row in df.iterrows():
            doc_data = {k: None if pd.isna(v) else v for k, v in row.to_dict().items()}
            key = f"{doc_data['Prénom']}_{doc_data['Nom']}"
            
            if key in existing_docs:
                # Mettre à jour le document existant
                batch.update(existing_docs[key], doc_data)
                del existing_docs[key]  # Retirer de la liste des documents existants
            else:
                # Créer un nouveau document
                new_doc_ref = collection_ref.document()
                batch.set(new_doc_ref, doc_data)
            
            batch_size += 1
            
            # Commiter le batch s'il atteint la limite
            if batch_size >= max_batch_size:
                batch.commit()
                batch = db.batch()
                batch_size = 0
        
        # Supprimer les documents qui n'existent plus dans le DataFrame
        for doc_ref in existing_docs.values():
            batch.delete(doc_ref)
            batch_size += 1
            
            if batch_size >= max_batch_size:
                batch.commit()
                batch = db.batch()
                batch_size = 0
        
        # Commiter les dernières modifications
        if batch_size > 0:
            batch.commit()
        
        return True
        
    except Exception as e:
        print(f"Erreur lors de la mise à jour de Firestore: {str(e)}")
        return False
