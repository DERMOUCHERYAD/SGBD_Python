class Condition:
    def __init__(self, term_left, operator, term_right, col_type=None):
       
        self.term_left = term_left #Terme gauche (peut être un indice de colonne ou une constante)
        self.operator = operator   #Opérateur de comparaison (ex: =, <, >, <=, >=, <>).
        self.term_right = term_right  #Terme droit (peut être un indice de colonne ou une constante).
        self.col_type = col_type  # Optionnel, utilisé pour comparer correctement les chaînes (ex: int, float, str) pour une gestion correcte.

    def evaluate(self, record, relation=None): #nom d'indice de colonne ou vérifier les types 
        """
        Évalue la condition pour un enregistrement donné.
        :param record: Liste des valeurs de l'enregistrement.
        :param relation: Instance de la relation pour obtenir les métadonnées (ex : types de colonnes).
        :return: True si la condition est vérifiée, False sinon.
        """
        # Récupérer les valeurs des termes gauche et droit
        left_value = self._get_value(self.term_left, record, relation)
        right_value = self._get_value(self.term_right, record, relation)

        # Effectuer la comparaison en fonction de l'opérateur
        return {
            "=": left_value == right_value,
            "<>": left_value != right_value,
            "<": left_value < right_value,
            ">": left_value > right_value,
            "<=": left_value <= right_value,
            ">=": left_value >= right_value,
        }.get(self.operator, False)  # si la clé n'existe pas  retourne False par default 

    def _get_value(self, term, record, relation=None):
        """
        Obtient la valeur réelle d'un terme.
        :param term: Peut être un indice de colonne ou une constante.
        :param record: Liste des valeurs de l'enregistrement.
        :param relation: Instance de la relation pour obtenir les indices de colonnes.
        :return: La valeur réelle du terme.
        """
        if isinstance(term, int):  # Si le terme est un indice de colonne , vérifie si un objet (obj) appartient à une classe ou un tuple de classes.
            return record[term]
        elif isinstance(term, str) and term.startswith('"'):  # Si le terme est une chaîne
            return term.strip('"') #netoie les " " et récupère la chaine tels qu'elle est 
        else:  # Sinon, c'est une constante (int ou float)
            return eval(term)  # Convertir le terme en sa valeur réelle (ex: "10" -> 10)

    def __str__(self):
        """
        Représentation en chaîne de la condition (utile pour le débogage).
        """
        return f"{self.term_left} {self.operator} {self.term_right}"
