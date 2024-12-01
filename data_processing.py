# data_processing.py

import unicodedata


def enlever_accents_avec_remplacement(texte):
    if isinstance(texte, str):
        texte_normalise = unicodedata.normalize('NFKD', texte)
        texte_sans_accents = ''.join(
            c if not unicodedata.combining(c) else '' for c in texte_normalise
        )
        remplacements_speciaux = {
            'ø': 'o', 'å': 'a', 'ä': 'a', 'ö': 'o', 'æ': 'ae',
            'č': 'c', 'š': 's', 'ž': 'z', 'ř': 'r',
            'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E',
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e',
        }
        for original, remplace in remplacements_speciaux.items():
            texte_sans_accents = texte_sans_accents.replace(original, remplace)
        return texte_sans_accents
    return texte
