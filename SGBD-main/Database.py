class Database:
    def __init__(self, name):
        self.name = name
        # Dictionnaire pour stocker les tables
        # Clé : Nom de la table, Valeur : Instance de la classe Relation
        self.tables = {}


     # Getter pour le nom de la base de données
    def get_name(self):
        return self.name

    # Setter pour le nom de la base de données
    def set_name(self, name):
        if not name:
            raise ValueError("Le nom de la base de données ne peut pas être vide.")
        self.name = name

    # Getter pour les tables
    def get_tables(self):
        return self.tables

    # Setter pour les tables (rarement utilisé, mais disponible si nécessaire)
    def set_tables(self, tables):
        if not isinstance(tables, dict):
            raise ValueError("Les tables doivent être un dictionnaire.")
        self.tables = tables


