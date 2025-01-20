# Importer la classe Record
from Record import Record  # Assurez-vous que le fichier de la classe est nommé record.py

# Tester la classe Record
def test_record():
    print("=== Test de la classe Record ===")

    # Test 1 : Création d'un record vide
    record = Record([])  # Crée un Record avec une liste vide

    print(f"Test 1: {record}")  # Doit afficher Record(valeurs=[])

    # Test 2 : Ajout de valeurs
    record.add_valeur(123)
    record.add_valeur("Hello")
    record.add_valeur(3.14)
    print(f"Test 2: {record}")  # Doit afficher Record(valeurs=[123, 'Hello', 3.14])

    # Test 3 : Accéder à une valeur
    print(f"Test 3: {record.get_valeur_at(1)}")  # Doit afficher 'Hello'

    # Test 4 : Modifier une valeur
    record.set_valeur_at(1, "World")
    print(f"Test 4: {record}")  # Doit afficher Record(valeurs=[123, 'World', 3.14])

    # Test 5 : Supprimer une valeur
    record.remove_valeur_at(2)
    print(f"Test 5: {record}")  # Doit afficher Record(valeurs=[123, 'World'])

    # Test 6 : Remplacer toutes les valeurs
    record.set_valeurs(["Nouvelle", "Liste", 42])
    print(f"Test 6: {record}")  # Doit afficher Record(valeurs=['Nouvelle', 'Liste', 42])

    # Test 7 : Gestion des erreurs
    try:
        record.get_valeur_at(10)
    except IndexError as e:
        print(f"Test 7: {e}")  # Doit afficher une erreur d'index

    try:
        record.set_valeur_at(10, "Erreur")
    except IndexError as e:
        print(f"Test 7: {e}")  # Doit afficher une erreur d'index

    try:
        record.remove_valeur_at(10)
    except IndexError as e:
        print(f"Test 7: {e}")  # Doit afficher une erreur d'index

if __name__ == "__main__":
    test_record()
