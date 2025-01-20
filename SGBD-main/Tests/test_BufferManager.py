import sys
import unittest
from unittest.mock import MagicMock
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from BufferManager import BufferManager

class TestBufferManager(unittest.TestCase):
    def setUp(self):
        self.db_config = MagicMock()
        self.db_config.bm_policy = "LRU"
        self.db_config.bm_buffercount = 2
        self.db_config.pageSize = 4096

        self.disk_manager = MagicMock()
        self.buffer_manager = BufferManager(self.db_config, self.disk_manager)

    def test_get_page_from_disk(self):
        page_id = 1
        self.disk_manager.ReadPage = MagicMock()
        buffer = self.buffer_manager.GetPage(page_id)
        self.disk_manager.ReadPage.assert_called_once_with(page_id, buffer)

    def test_get_page_from_buffer(self):
        page_id = 1
        buffer = bytearray(self.db_config.pageSize)
        self.buffer_manager.buffer_pool.append((page_id, buffer))
        result = self.buffer_manager.GetPage(page_id)
        self.assertEqual(result, buffer)

    def test_free_page(self):
        page_id = 1
        buffer = bytearray(self.db_config.pageSize)
        self.buffer_manager.buffer_pool.append((page_id, buffer, 1, False))
        self.buffer_manager.FreePage(page_id, True)
        self.assertEqual(self.buffer_manager.buffer_pool[0], (page_id, buffer, 0, True))

    def test_set_current_replacement_policy(self):
        self.buffer_manager.SetCurrentReplacementPolicy("MRU")
        self.assertEqual(self.buffer_manager.replacement_policy, "MRU")

    # def test_flush_buffers(self):
    #     page_id = 1
    #     buffer = bytearray(self.db_config.pageSize)
    #     self.buffer_manager.buffer_pool.append((page_id, buffer, 0, True))
    #     self.disk_manager.WritePage = MagicMock()
    #     self.buffer_manager.FlushBuffers()
    #     self.disk_manager.WritePage.assert_called_once_with(page_id, buffer)x

if __name__ == '__main__':
    unittest.main()