import json
import os 

class DBConfig:
    def __init__(self, dbpath ,pageSize ,dm_maxfilesize ,bm_buffercount , bm_policy):
        self.dbpath = dbpath
        self.pageSize = pageSize
        self.dm_maxfilesize = dm_maxfilesize
        self.bm_buffercount = bm_buffercount
        self.bm_policy = bm_policy

      # Méthode __repr__ pour la représentation en chaîne de l'objet
    def __repr__(self):
        """
        Représentation de l'objet DBConfig, affichant les informations de configuration.
        """
        return f"DBConfig(dbpath='{self.dbpath}', pageSize={self.pageSize}, dm_maxfilesize={self.dm_maxfilesize})"

    

    def get_dbpath(self):
        return self.dbpath

    def set_dbpath(self, dbpath):
        self.dbpath = dbpath

    def get_pageSize(self):
        return self.pageSize

    def set_pageSize(self, pageSize):
        self.pageSize = pageSize

    def get_dm_maxfilesize(self):
        return self.dm_maxfilesize

    def set_dm_maxfilesize(self, dm_maxfilesize):
        self.dm_maxfilesize = dm_maxfilesize

    def get_bm_buffercount(self):
            return self.bm_buffercount

    def get_bm_policy(self):
        return self.bm_policy
        
        #Le type de méthode LoadDBConfig : Vous l'avez définie comme une méthode classique de la classe, mais elle n'utilise pas l'instance (self).
        #Cela signifie qu'elle devrait être définie comme une méthode statique (ou classe) en utilisant @staticmethod.......
    
    @staticmethod    
    def load_db_config(file_path):
        try:
            with open(file_path, 'r') as file:
                config=json.load(file)
                return DBConfig(config["dbpath"] ,config["pageSize"] ,config["dm_maxfilesize"] ,config["bm_buffercount"], config["bm_policy"])
                
        except Exception as openFail:
            print(f"erreur d'ouverture de fichier = {openFail}")
        

