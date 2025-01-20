import os
import traceback
import sys

import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from SGBD import SGBD
from DBConfig import DBConfig

class SGBDTest:
    @staticmethod
    def run_tests():
        try:
            
            
            
            db_config = DBConfig.load_db_config("config.json")  # Load DBConfig from a file
            sgbd = SGBD(db_config)

            # Test the SGBD
            """ 
            Creation de la base de données et de la table ; insertion d'un enregistrement ;
            insertion en masse ; 
            récupération de la table ; 
            liste des tables ; 
            suppression de la table ; 
            suppression de la base de données;
            
            """
            
            print("\n--- Test 1 : Création de base de données ---") 
            sgbd.ProcessCreateDatabaseCommand("CREATE DATABASE TestDB")
            print("Base de données 'TestDB' créée avec succès.\n")

            print("\n--- Test 2 : Sélection de la base de données courante ---")
            sgbd.ProcessSetCurrentDatabaseCommand("SET DATABASE TestDB")
            print("Base de données 'TestDB' définie comme base de données courante.\n")

            print("\n--- Test 3 : Création de table ---")
            sgbd.ProcessAddTableToCurrentDatabaseCommand("CREATE TABLE Users (ID:INT, Name:VARCHAR(50), Age:INT)")
            print("Table 'Users' créée avec succès dans 'TestDB'.\n")

            print("\n--- Test 4 : Insertion d'un enregistrement ---")
            sgbd.processInsertCommand(["INTO", "Users", "VALUES", "(1, 'Alice', 25)"])
            print("Enregistrement inséré avec succès dans 'Users'.\n")

            print("\n--- Test 5 : Insertion en masse ---")
            with open("users.csv", "w") as f:
                f.write("2, 'Bob', 30\n")
                f.write("3, 'Charlie', 35\n")
                f.write("4, 'David', 40\n")
            sgbd.processBulkInsertCommand(["BULKINSERT", "INTO", "Users", "users.csv"])
            print("Insertion en masse réalisée avec succès pour 'Users'.\n")

            print("\n--- Test 6 : Récupération de table ---")
            sgbd.ProcessGetTableFromCurrentDatabaseCommand("GET TABLE Users")
            print("Table 'Users' récupérée avec succès.\n")

            print("\n--- Test 7 : Liste des tables ---")
            sgbd.ProcessListDatabasesCommand("LIST TABLES")
            print("Tables listées avec succès dans 'TestDB'.\n")

            print("\n--- Test 8 : Suppression de table ---")
            sgbd.ProcessRemoveTableFromCurrentDatabaseCommand("DROP TABLE Users")
            print("Table 'Users' supprimée avec succès de 'TestDB'.\n")

            print("\n--- Test 9 : Suppression de la base de données ---")
            sgbd.ProcessRemoveDatabaseCommand("DROP DATABASE TestDB")
            print("Base de données 'TestDB' supprimée avec succès.\n")

        except Exception as e:
            print("Une erreur est survenue lors du test :")
            print(traceback.format_exc())

        finally:
            if os.path.exists("users.csv"):
                os.remove("users.csv")
# Run the tests
if __name__ == "__main__":
    SGBDTest.run_tests()
