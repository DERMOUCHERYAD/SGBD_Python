class Record:
    def __init__(self, valeurs=None):
        """
        Initialise un Record avec une liste de valeurs.
        :param valeurs: Liste de valeurs pour initialiser le record. Si None, initialise une liste vide.
        """
        if valeurs is None:
            self.valeurs = []  # Initialiser avec une liste vide si aucune valeur fournie
        elif isinstance(valeurs, list):
            self.valeurs = valeurs  # Assurez-vous que c'est une liste
        else:
            raise ValueError("Les valeurs doivent être une liste ou None.")

    def __repr__(self):
        """
        Représentation en chaîne de l'objet Record pour le debug.
        """
        return f"Record(valeurs={self.valeurs})"

    def get_valeurs(self):
        """
        Retourne la liste des valeurs du record.
        """
        return self.valeurs

    def set_valeurs(self, valeurs):
        """
        Modifie la liste de valeurs du record.
        :param valeurs: Nouvelle liste de valeurs.
        """
        if not isinstance(valeurs, list):
            raise ValueError("Les valeurs doivent être une liste.")
        self.valeurs = valeurs

    def get_valeur_at(self, index):
        """
        Retourne la valeur à l'index spécifié.
        :param index: Index de la valeur à retourner.
        :return: Valeur à l'index donné.
        """
        if index < 0 or index >= len(self.valeurs):
            raise IndexError("Index hors limites.")
        return self.valeurs[index]

    def set_valeur_at(self, index, valeur):
        """
        Modifie la valeur à un index spécifié.
        :param index: Index à modifier.
        :param valeur: Nouvelle valeur à mettre.
        """
        if index < 0 or index >= len(self.valeurs):
            raise IndexError("Index hors limites.")
        self.valeurs[index] = valeur

    def add_valeur(self, valeur):
        """
        Ajoute une valeur à la fin du record.
        :param valeur: Valeur à ajouter.
        """
        self.valeurs.append(valeur)

    def remove_valeur_at(self, index):
        """
        Supprime la valeur à l'index donné.
        :param index: Index de la valeur à supprimer.
        """
        if index < 0 or index >= len(self.valeurs):
            raise IndexError("Index hors limites.")
        self.valeurs.pop(index)
