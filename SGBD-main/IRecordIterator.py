class IRecordIterator:
    def GetNextRecord(self):
        """
        Retourne le record courant et avance le curseur de l'itérateur.
        Retourne None lorsqu'il n'y a plus de records à parcourir.
        """
        raise NotImplementedError("La méthode GetNextRecord doit être implémentée.") #c'est possible de lever les exceptions en python ;)

    def Close(self):
        """
        Signale que l'itérateur n'est plus utilisé et libère les ressources associées.
        """
        raise NotImplementedError("La méthode Close doit être implémentée.")

    def Reset(self):
        """
        Remet le curseur au début de l'ensemble des records.
        """
        raise NotImplementedError("La méthode Reset doit être implémentée.")
