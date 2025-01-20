from IRecordIterator import IRecordIterator
from Relation import Relation
from Record import Record

class RelationScanner(IRecordIterator):
    def __init__(self, relation):
        """
        Initialise le scanner pour parcourir les tuples d'une relation.
        :param relation: Une instance de la classe Relation.
        """
        self.relation = relation
        self.records = relation.getAllRecords()  # Récupérer tous les tuples
        self.current_index = 0  # Initialise l'indice du record courant

    def GetNextRecord(self):
        """
        Retourne le prochain tuple de la relation et avance le curseur.
        Retourne None lorsqu'il n'y a plus de tuples.
        """
        if self.current_index < len(self.records):
            record = self.records[self.current_index]
            self.current_index += 1
            return record
        return None  # Aucun record restant

    def Reset(self):
        """
        Réinitialise le scanner pour recommencer depuis le début.
        """
        self.current_index = 0

    def Close(self):
        """
        Libère les ressources associées au scanner.
        Dans cette implémentation, il n'y a pas de ressources spécifiques à libérer.
        """
        self.current_page_index = 0
        self.current_record_index = 0
        if self.data_pages:
            self._load_page(self.data_pages[0])
