from Relation import Relation
from Record import Record
from ColInfo import ColInfo
from pageId import PageId
from DiskManager import DiskManager
from BufferManager import BufferManager
from DBConfig import DBConfig
import struct

def test_relation_methods():
    print("=== Test des méthodes de Relation ===")

    # Configuration de test
    dbConfig = DBConfig(
        dbpath="test_db",
        pageSize=4096,
        dm_maxfilesize=10000,
        bm_buffercount=5,
        bm_policy="LRU"
    )

    # Initialiser DiskManager et BufferManager
    diskManager = DiskManager(dbConfig)
    bufferManager = BufferManager(dbConfig, diskManager)

    # Définir le schéma de la relation
    colInfoList = [
        ColInfo("col1", "INT"),
        ColInfo("col2", "CHAR(10)"),
        ColInfo("col3", "INT")
    ]

    # Créer la relation
    headerPageId = PageId(0, 0)
    relation = Relation("TestRelation", 3, colInfoList, False, headerPageId, diskManager, bufferManager)

    # Test: Ajouter une page de données
    print("\n--- Test: addDataPage ---")
    relation.addDataPage()
    print("Page de données ajoutée avec succès.")

    # Test: Trouver une page libre pour un record
    print("\n--- Test: getFreeDataPageId ---")
    pageId = relation.getFreeDataPageId(100)
    if pageId:
        print(f"Page libre trouvée: {pageId}")
    else:
        print("Aucune page libre trouvée.")

    # Test: Insérer un record
    print("\n--- Test: InsertRecord ---")
    record = Record([1, "testvalue", 42])
    rid = relation.InsertRecord(record)
    print(f"Record inséré avec RecordId: {rid}")

    # Test: Récupérer les records d'une page de données
    print("\n--- Test: getRecordsInDataPage ---")
    records = relation.getRecordsInDataPage(rid.pageId)
    print("Records récupérés:")
    for rec in records:
        print(rec.get_valeurs())

    # Test: Lister toutes les pages de données
    print("\n--- Test: getDataPages ---")
    dataPages = relation.getDataPages()
    print("Pages de données:")
    for page in dataPages:
        print(page)

    # Test: Récupérer tous les records
    print("\n--- Test: GetAllRecords ---")
    allRecords = relation.GetAllRecords()
    print("Tous les records:")
    for rec in allRecords:
        print(rec.get_valeurs())

if __name__ == "__main__":
    test_relation_methods()
