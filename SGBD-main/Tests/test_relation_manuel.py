import struct
from pageId import PageId
from Relation import Relation
from Record import Record
from RecordId import RecordId
from ColInfo import ColInfo

# Mocks pour remplacer les dépendances externes
class MockDBConfig:
    def __init__(self, dbpath, pageSize, dm_maxfilesize, bm_buffercount, bm_policy):
        self.dbpath = dbpath
        self.pageSize = pageSize
        self.dm_maxfilesize = dm_maxfilesize
        self.bm_buffercount = bm_buffercount
        self.bm_policy = bm_policy

    def get_dbpath(self):
        return self.dbpath

    def get_pageSize(self):
        return self.pageSize

    def get_dm_maxfilesize(self):
        return self.dm_maxfilesize

    def get_bm_buffercount(self):
        return self.bm_buffercount

    def get_bm_policy(self):
        return self.bm_policy

class MockDiskManager:
    def __init__(self, dbc):
        self.dbc = dbc
        self.pages = {}
        self.file_count = 0

    def getDBC(self):
        return self.dbc

    def AllocPage(self):
        page_id = PageId(self.file_count, 0)
        self.pages[(page_id.FileIdx, page_id.PageIdx)] = bytearray(self.dbc.get_pageSize())
        self.file_count += 1
        return page_id

class MockBufferManager:
    def __init__(self, dbConfig, disk_manager):
        self.db_config = dbConfig
        self.disk_manager = disk_manager
        self.buffer_pool = []

    def getPage(self, pageId):
        key = (pageId.FileIdx, pageId.PageIdx)
        if key not in self.disk_manager.pages:
            raise Exception("Page demandée inexistante!")
        for i, p in enumerate(self.buffer_pool):
            if p is self.disk_manager.pages[key]:
                return i
        self.buffer_pool.append(self.disk_manager.pages[key])
        return len(self.buffer_pool) - 1

    def FreePage(self, pageId, valdirty=False):
        pass  # Pas d'action nécessaire pour ce mock

# Test manuel pour la classe Relation
def test_relation_manuel():
    # Création d'une configuration et des mocks
    dbc = MockDBConfig(dbpath="./db", pageSize=4096, dm_maxfilesize=100, bm_buffercount=5, bm_policy="LRU")
    dm = MockDiskManager(dbc)
    bm = MockBufferManager(dbc, dm)

    # Création d'une Header Page vide
    headerPageId = dm.AllocPage()
    header_buff = dm.pages[(headerPageId.FileIdx, headerPageId.PageIdx)]
    struct.pack_into('i', header_buff, 0, 0)  # nb_pages=0

    # Définition de colonnes : un INT et un CHAR(10)
    col1 = ColInfo("id", "INT")
    col2 = ColInfo("name", "CHAR(10)")

    # Création de la relation
    relation = Relation("test_relation", 2, [col1, col2], False, headerPageId, dm, bm)

    print("== Test Insertion Record ==")
    r1 = Record(["1", "Alice"])
    rid1 = relation.InsertRecord(r1)
    print(f"Insertion record: {r1.get_valeurs()}, RID = {rid1.slotIdx}")

    r2 = Record(["2", "Bob"])
    rid2 = relation.InsertRecord(r2)
    print(f"Insertion record: {r2.get_valeurs()}, RID = {rid2.slotIdx}")

    print("\n== Test GetAllRecords ==")
    all_records = relation.GetAllRecords()
    print("Records:", [rec.get_valeurs() for rec in all_records])

    print("\n== Test GetDataPages ==")
    data_pages = relation.getDataPages()
    print("Data Pages:", [(p.FileIdx, p.PageIdx) for p in data_pages])

    print("\n== Test getRecordsInDataPage ==")
    page_records = relation.getRecordsInDataPage(data_pages[0])
    print("Records in page:", [r.get_valeurs() for r in page_records])

    print("\nTous les tests se sont déroulés avec succès!")

if __name__ == "__main__":
    test_relation_manuel()
