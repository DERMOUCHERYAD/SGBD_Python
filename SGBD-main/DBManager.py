import json
import os
from Database import Database
from Relation import Relation
from ColInfo import ColInfo
from pageId import PageId

class DBManager:
    def __init__(self, db_config):
      
        self.db_config = db_config  # Instance de DBConfig
        self.databases = {}  # Dictionnaire pour stocker les bases de données (nom -> Database)
        self.active_database = None  # Base de données couramment active    # l'attribut self.active_database est défini,il ne prend pas simplement le nom de la base de données courante, mais une référence à l'instance de la classe Database représentant la base active.
                                                                              
                                                                              
    def CreateDatabase(self, nom_bdd):
    #Crée une nouvelle base de données avec le nom spécifié.
        if nom_bdd in self.databases:
            raise ValueError(f"La base de données '{nom_bdd}' existe déjà.")
    # Ajouter une nouvelle base de données (instance de Database) au dictionnaire
        self.databases[nom_bdd] = Database(nom_bdd)
        print(f"Base de données '{nom_bdd}' créée avec succès.")

    def SetCurrentDatabase(self, nom_bdd):
    # Active la base de données spécifiée par son nom.
        if nom_bdd not in self.databases:
            raise ValueError(f"La base de données '{nom_bdd}' n'existe pas.")
    # Définir la base de données active
        self.active_database = self.databases[nom_bdd]
        print(f"Base de données active : '{nom_bdd}'.")

    def AddTableToCurrentDatabase(self, tab):
    #Ajoute une table à la base de données active.
        if not self.active_database:
            raise ValueError("Aucune base de données active. Veuillez activer une base de données avant d'ajouter une table.")
        table_name = tab.relationName  # Nom de la table à partir de l'instance Relation
        if table_name in self.active_database.tables: 
            raise ValueError(f"La table '{table_name}' existe déjà dans la base de données active.")
    # Ajout de la table dans la base active
        self.active_database.tables[table_name] = tab
        print(f"Table '{table_name}' ajoutée à la base de données '{self.active_database.name}'.")

    def GetTableFromCurrentDatabase(self, nom_table):
    #Retourne l'instance de Relation correspondant à nomTable dans la base de données active.
        if not self.active_database:
            raise ValueError("Aucune base de données active.")
        if nom_table not in self.active_database.tables:
            raise ValueError(f"La table '{nom_table}' n'existe pas dans la base de données active.")
        return self.active_database.tables[nom_table]

    def RemoveTableFromCurrentDatabase(self, nom_table):
    #Supprime la table correspondant à nomTable dans la base de données active.
        if not self.active_database:
            raise ValueError("Aucune base de données active.")
        if nom_table not in self.active_database.tables:
            raise ValueError(f"La table '{nom_table}' n'existe pas dans la base de données active.")
        del self.active_database.tables[nom_table] #Supression
        print(f"La table '{nom_table}' a été supprimée de la base de données '{self.active_database.name}'.")


    def RemoveDatabase(self, nomBdd):
    #Supprime la base de données correspondant à nomBdd.
        if nomBdd not in self.databases:
            raise ValueError(f"La base de données '{nomBdd}' n'existe pas.")
     #DESACTIVER LA BASE DE DONNES AVANT LA SUPRESSION 
        if self.active_database and self.active_database.name == nomBdd: #verifier si elle est active et c'est bien la BDD qu'on veut supprimer 
            self.active_database = None  # Désactiver la base 
        del self.databases[nomBdd]
        print(f"La base de données '{nomBdd}' a été supprimée.")

    def RemoveTablesFromCurrentDatabase(self):
    #Supprime toutes les tables de la base de données active en utilisant clear().
        if not self.active_database:
            raise ValueError("Aucune base de données active.")
    
        self.active_database.tables.clear()
        print(f"Toutes les tables de la base de données '{self.active_database.name}' ont été supprimées.")

    def RemoveDatabases(self):
    #Supprime toutes les bases de données en utilisant clear() et désactive la base active.
        self.databases.clear()
        self.active_database = None #desactiver la reference car y aura aucune BDD active 
        print("Toutes les bases de données ont été supprimées.")

    def ListDatabases(self):
    #Affiche les noms de toutes les bases de données existantes, une par ligne.
        if not self.databases:
            print("Aucune base de données existante.")
        else:
            print("Bases de données existantes :")
            for db_name in self.databases.keys(): #liste des clés de bases de données qui existent 
                print(db_name)

    def ListTablesInCurrentDatabase(self):
    #Affiche les noms et les schémas (noms et types des colonnes) de toutes les tables de la base de données couramment active, une table par ligne.
        if not self.active_database:
            raise ValueError("Aucune base de données active.")
        if not self.active_database.tables:
            print(f"La base de données '{self.active_database.name}' ne contient aucune table.")
            return
        print(f"Tables dans la base de données '{self.active_database.name}':")
        for table_name, relation in self.active_database.tables.items(): # parcourir le dictionnaire pour avoir des paires (clé,valeur) sous forme de tuples 
        # Construire le schéma à partir des colonnes de la relation
            schema = ""
            for i in range(len(relation.col_info_list)):
                if(i == len(relation.col_info_list)-1):
                    schema += (relation.col_info_list[i].toString())
                else:
                    schema += (relation.col_info_list[i].toString()) + ", " #concaténer les éléments de la liste en la sépareant par ,
            print(f" TABLE {table_name} ({schema})")

    def SaveState(self):
    #Sauvegarde l'état actuel des bases de données et des tables dans un fichier JSON.
        save_path = os.path.join(self.db_config.get_dbpath(), "databases.save") #Construit le chemin vers le fichier databases.save en le plaçant dans le répertoire défini par dbpath.
        data_to_save = {"databases": {}} #les accolades extérieures définissent le contenu global du fichier JSON 
        for db_name, db_instance in self.databases.items():
            db_info = {"tables": {}}   #Crée un dictionnaire db_info pour stocker les informations des tables de la base courante.
            for table_name, relation in db_instance.tables.items(): #Parcourt chaque table de la base courante (db_instance.tables).    
                db_info["tables"][table_name] = { #créer un dictionnaire dans le dictionnaires des tables 
                    "columns": [ci.to_dict() for ci in relation.col_info_list], #Liste des colonnes et leurs types
                    "headerPageId": relation.headerPageId.to_dict(), #Identifiant de la page d'en-tête de la table    
                }
            data_to_save["databases"][db_name] = db_info    #Ajoute les informations de la base courante (db_info) sous la clé correspondant à son nom (db_name) dans data_to_save qui represente notre JSON
        try:
            with open(save_path, "w") as save_file: 
                json.dump(data_to_save, save_file, indent=4) #Écrit le contenu de data_to_save dans le fichier, en format JSON et indent=4 formate le fichier pour qu'il soit lisible (avec une indentation de 4 espaces).
                save_file.close()
            print(f"État sauvegardé dans {save_path}")
        except Exception as e:
            print(f"Erreur lors de la sauvegarde : {e}")
            

    def LoadState(self, diskManager):
    #Charge l'état des bases de données et des tables à partir du fichier de sauvegarde JSON.
        save_path = os.path.join(self.db_config.get_dbpath(), "databases.save") #prépare l'endroit où le fichier devrait se trouver
        if not os.path.exists(save_path):
            print(f"Fichier de sauvegarde introuvable : {save_path}")
            return
        try:
            with open(save_path, "r") as save_file:
                loaded_data = json.load(save_file) #charger le contenu du fichier Json dans un dictionnaire Python 
            for db_name, db_info in loaded_data.get("databases", {}).items(): #Parcourir chaque base de données enregistrée dans le fichier
                # Créer la base de données
                new_db = Database(db_name)
                for table_name, table_info in db_info["tables"].items():
                    # Créer la relation avec les colonnes et le headerPageId
                    columns = [ColInfo(col["colNom"], col["colType"]) for col in table_info["columns"]]  # Reconstruire ColInfo
                    heads = PageId(table_info["headerPageId"]["FileIdx"],table_info["headerPageId"]["PageIdx"])  # Reconstruire ColInfo
                    new_relation = Relation(
                        table_name,
                        len(columns),
                        columns, #Liste des colonnes 
                        False,  # Suppose tailleVar est False
                        heads,
                        diskManager,
                        None  # bufferManager non nécessaire ici
                    )
                    new_db.tables[table_name] = new_relation #ajouter la nouvelle relation a la base de courante 
                self.databases[db_name] = new_db             #Ajouter la base de données courante (new_db) au dictionnaire des bases (self.databases)
            
            if os.path.getsize(save_path) != 2:
                print(f"État chargé depuis {save_path}")
        except Exception as e:
            print(f"Erreur lors du chargement : {e}")
   
   