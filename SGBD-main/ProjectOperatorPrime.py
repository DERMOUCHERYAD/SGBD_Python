class ProjectOperatorPrime(IRecordIterator):
    def __init__(self, relation, column_indices, condition):
        """
        Initialise l'opérateur de projection.
        :param relation: Une instance de la classe Relation.
        :param column_indices: Liste des indices des colonnes à conserver.
        :param condition: Une instance de la classe Condition pour filtrer les tuples.
        """
        self.column_indices = column_indices
        self.child = RelationScannerWithSelect(relation, condition)  # Utilise RelationScannerWithSelect directement

    def GetNextRecord(self):
        """
        Retourne le prochain tuple projeté et avance le curseur.
        Retourne None lorsqu'il n'y a plus de tuples.
        """
        while True:
            record = self.child.GetNextRecord()
            if record is None:
                return None

            # Projeter uniquement les colonnes nécessaires
            projected_values = [record.get_valeurs()[index] for index in self.column_indices]
            return Record(projected_values)

    def Reset(self):
        """
        Réinitialise le ProjectOperator pour recommencer depuis le début.
        """
        self.child.Reset()

    def Close(self):
        """
        Libère les ressources associées à l'opérateur de projection.
        """
        self.child.Close()
