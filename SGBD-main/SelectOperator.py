class SelectOperator(IRecordIterator):
    #Filtrer les tuples en fonction d'une condition
    def __init__(self, child, condition):
        """
        child : IRecordIterator
            Opérateur fils qui fournit les tuples 
        """
        self.child = child #child est une instance de la classe ScannerRelation, 
        self.condition = condition

    def GetNextRecord(self):
        """
        Récupère le prochain tuple qui satisfait la condition.
        """
        while True: #de la l'evaluation de la condition 
            record = self.child.GetNextRecord() 
            if record is None:
                return None  # Plus de tuples à parcourir
            if self.condition.evaluate(record):
                return record  # Retourne le tuple valide

    def Close(self):
        """
        Libère les ressources associées à l'opérateur fils.
        """
        self.child.Close()

    def Reset(self):
        """
        Réinitialise l'opérateur fils.
        """
        self.child.Reset()
