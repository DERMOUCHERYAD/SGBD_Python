import sys
import unittest
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DiskManager import DiskManager
from pageId import PageId

# Mock dbc class to simulate database configuration
class MockDbc:
    def __init__(self, dbpath, pageSize, dm_maxfilesize):
        self.dbpath = dbpath
        self.pageSize = pageSize
        self.dm_maxfilesize = dm_maxfilesize

class TestDiskManager(unittest.TestCase):
    def setUp(self):
        self.dbc = MockDbc(dbpath="test_db", pageSize=1024, dm_maxfilesize=4096)
        self.disk_manager = DiskManager(self.dbc)
        os.makedirs(os.path.join(self.dbc.dbpath, "BinData"), exist_ok=True)

    def tearDown(self):
        # Clean up test files
        if os.path.exists(self.dbc.dbpath):
            for root, dirs, files in os.walk(self.dbc.dbpath, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(self.dbc.dbpath)

    # def test_alloc_page_reuses_deallocated_pages(self):
    #     # Allocate and deallocate pages
    #     page_ids = [self.disk_manager.AllocPage() for _ in range(5)]
    #     for page_id in page_ids:
    #         self.disk_manager.DeallocPage(page_id)

    #     # Allocate pages again and ensure the same pages are reused
    #     reused_page_ids = [self.disk_manager.AllocPage() for _ in range(5)]
    #     self.assertEqual(page_ids, reused_page_ids, "Deallocated pages were not reused")

    def test_alloc_page_handles_no_free_pages(self):
        # Ensure no free pages
        self.disk_manager.free_pages = []
        self.disk_manager.SaveState()

        # Allocate a page
        page_id = self.disk_manager.AllocPage()
        self.assertIsNotNone(page_id, "AllocPage returned None when no free pages")

    def test_alloc_page_creates_new_file_when_last_is_full(self):
        # Fill up the first file

        for _ in range(self.dbc.dm_maxfilesize//self.dbc.pageSize):
            page_id = self.disk_manager.AllocPage()
            self.disk_manager.WritePage(page_id, b"Hello, World! ")
            self.assertEqual(page_id.FileIdx, 0, "Page allocated in wrong file")

        # Next allocation should create a new file
        new_page_id = self.disk_manager.AllocPage()
        self.assertEqual(new_page_id.FileIdx, 1, "New file was not created when last was full")
        self.assertEqual(new_page_id.PageIdx, 0, "First page in new file should have PageIdx 0")

if __name__ == '__main__':
    unittest.main()