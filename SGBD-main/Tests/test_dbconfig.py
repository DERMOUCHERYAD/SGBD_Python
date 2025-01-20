import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DBConfig import DBConfig 


 
def test_load_db_config_valid():
    print("\n--- Test1: Chargement de la configuration valide ? ---")
    config = DBConfig.load_db_config("config.json")
    if config :
        print("Test réussi : Le chemin a été chargé correctement.")
    else:
        print("Test échoué : Le chemin n'a pas été chargé comme prévu.")

def test_load_db_config_file_not_found():
    print("\n--- Test2: Fichier de configuration introuvable ---")
    config = DBConfig.load_db_config("inexistant_config.json")  # Nom d'un fichier Json qui n'existe pas
    if config is None:
        print("Test réussi : Erreur gérée pour le fichier introuvable.")
    else:
        print("Test échoué : La configuration a été chargée alors qu'elle ne devrait pas.")

def test_load_db_config_invalid_json():
    print("\n--- Test3: Fichier de configuration avec JSON invalide ---")
    with open("invalid_config.json", 'w') as file:
        file.write("{invalid_json}")  # Écriture d'un JSON invalide

    config = DBConfig.load_db_config("invalid_config.json")
    if config is None:
        print("Test réussi : Erreur gérée pour le JSON invalide.")
    else:
        print("Test échoué : La configuration a été chargée alors qu'elle ne devrait pas.")

    # Supprimer le fichier de test après le test
    os.remove("invalid_config.json")

def main():
    test_load_db_config_valid()
    test_load_db_config_file_not_found()
    test_load_db_config_invalid_json()

if __name__ == "__main__":
    main()
