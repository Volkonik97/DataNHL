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
    Met à jour une collection Firestore avec les données d'un DataFrame
    """
    if not firebase_admin._apps:
        initialize_firebase()
    
    db = firestore.client()
    collection_ref = db.collection(collection_name)
    
    # Supprimer tous les documents existants
    docs = collection_ref.stream()
    for doc in docs:
        doc.reference.delete()
    
    # Ajouter les nouvelles données
    for _, row in df.iterrows():
        # Convertir la ligne en dictionnaire et nettoyer les valeurs NaN
        doc_data = row.to_dict()
        # Remplacer les valeurs NaN par None pour Firestore
        doc_data = {k: None if pd.isna(v) else v for k, v in doc_data.items()}
        collection_ref.add(doc_data)
