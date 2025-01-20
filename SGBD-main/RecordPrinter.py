class RecordPrinter:
    def __init__(self, iterator):
        """
        Initialise le RecordPrinter avec un itérateur de tuples.
        :param iterator: Une instance implémentant IRecordIterator.
        """
        self.iterator = iterator

    def print_records(self):
        """
        Parcourt et affiche les tuples fournis par l'itérateur, ainsi que le total des records.
        """
        count = 0  # Compteur de records

        while True:
            record = self.iterator.GetNextRecord()
            if record is None:
                break  # Plus de records

            # Afficher les valeurs du record, séparées par des ";" et terminées par un point
            values = record.get_valeurs()
            print("; ".join(map(str, values)) + ".")
            count += 1

        print(f"Total records={count}")

    def reset_and_print(self):
        """
        Réinitialise l'itérateur et réimprime tous les records.
        """
        self.iterator.Reset()
        self.print_records()

    def close(self):
        """
        Ferme l'itérateur et libère les ressources associées.
        """
        self.iterator.Close()

