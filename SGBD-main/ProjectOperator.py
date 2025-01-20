class ProjectOperator(IRecordIterator):
    def __init__(self, child, column_indices):
        """
        child : IRecordIterator
            Opérateur fils qui fournit les tuples.
        column_indices : list[int]
            Indices des colonnes à conserver.
        """
        self.child = child #child est une instance de la classe SelectOperator
        self.column_indices = column_indices

    def GetNextRecord(self):
        """
        Récupère le prochain tuple et applique la projection pour garder les colonnes demandées.
        """
        record = self.child.GetNextRecord()  #Obtenir un tuple du fils ( objet de la classe SelecOperator
        if record is None:
            return None  # Plus de tuples à parcourir
        # Appliquer la projection
        projected_values = [record.get_valeurs()[index] for index in self.column_indices]   #liste des indices des colonnes à inclure dans la projection
        return Record(projected_values) #construction d'un nouveau tuple qui contient des valeurs des colonnes correspondant à ces indices.

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
