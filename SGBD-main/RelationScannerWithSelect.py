from IRecordIterator import IRecordIterator
from Relation import Relation
from Condition import Condition

class RelationScannerWithSelect(IRecordIterator):
    def __init__(self, relation, condition):
        """
        Initialise un scanner avec une condition pour filtrer les tuples lors de la lecture.
        :param relation: Une instance de la classe Relation.
        :param condition: Une instance de la classe Condition pour filtrer les tuples.
        """
        self.relation = relation
        self.condition = condition
        self.data_pages = self.relation.getDataPages()  # Liste des pages de données
        self.current_page_index = 0  # Indice de la page courante
        self.current_page_records = []  # Tuples de la page courante
        self.current_record_index = 0  # Indice du tuple courant

        self._load_next_page()

    def _load_next_page(self):
        """
        Charge les enregistrements de la page courante.
        Avance à la page suivante si nécessaire.
        """
        while self.current_page_index < len(self.data_pages):
            page_id = self.data_pages[self.current_page_index]
            self.current_page_records = self.relation.getRecordsInDataPage(page_id)
            self.current_page_index += 1
            self.current_record_index = 0

            if self.current_page_records:
                return  # Une nouvelle page avec des tuples a été chargée

        # Plus de pages ou de tuples à lire
        self.current_page_records = []

    def GetNextRecord(self):
        """
        Retourne le prochain tuple filtré selon la condition et avance le curseur.
        Retourne None lorsqu'il n'y a plus de tuples.
        """
        while self.current_page_records:
            if self.current_record_index < len(self.current_page_records):
                record = self.current_page_records[self.current_record_index]
                self.current_record_index += 1

                # Appliquer le filtre de condition
                if self.condition.evaluate(record.get_valeurs(), self.relation):
                    return record

            else:
                # Charger la prochaine page si tous les tuples ont été parcourus
                self._load_next_page()

        return None  # Aucun tuple restant

    def Reset(self):
        """
        Réinitialise le scanner pour recommencer depuis le début.
        """
        self.current_page_index = 0
        self.current_record_index = 0
        self.current_page_records = []
        self._load_next_page()

    def Close(self):
        """
        Libère les ressources associées au scanner.
        Dans cette implémentation, il n'y a pas de ressources spécifiques à libérer.
        """
        self.data_pages = []
        self.current_page_records = []
        self.current_page_index = 0
        self.current_record_index = 0
