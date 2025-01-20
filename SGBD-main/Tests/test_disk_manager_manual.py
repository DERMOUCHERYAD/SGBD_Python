import os
from DiskManager import DiskManager, PageId  # Assuming PageId is a class
from BufferManager import BufferManager

class MockDbc:
    def __init__(self, dbpath, dm_maxfilesize, pageSize):
        self.dbpath = dbpath
        self.dm_maxfilesize = dm_maxfilesize
        self.pageSize = pageSize

def manual_test_disk_manager():
    # Set up a mock configuration for the database
    db_config = MockDbc(dbpath="test_db", dm_maxfilesize=4096, pageSize=512)
    disk_manager = DiskManager(db_config)

    # Ensure the directory structure exists
    os.makedirs(f"./{db_config.dbpath}/BinData", exist_ok=True)
    
    # Test AllocPage method
    print("\nTesting AllocPage:")
    allocated_page = DiskManager.AllocPage()
    
    
    # Test DeallocPage method
    print("\nTesting DeallocPage:")
    disk_manager.DeallocPage(allocated_page)
    
    
    # Test WritePage method
    print("\nTesting WritePage:")
    buffer = bytearray(b"Hello, DiskManager!")  # Example content to write
    disk_manager.WritePage(allocated_page, buffer)
    print(f"Written '{buffer}' to page {allocated_page}")
    
    # Test ReadPage method
    print("\nTesting ReadPage:")
    read_buffer = bytearray(db_config.pageSize)
    disk_manager.ReadPage(allocated_page, read_buffer)
    print(f"Read from page {allocated_page}: {read_buffer[:len(buffer)]}")

    # Test SaveState method
    print("\nTesting SaveState:")
    disk_manager.SaveState()
    print("State saved to file 'dm.save'.")

    # Test LoadState method
    print("\nTesting LoadState:")
    new_disk_manager = DiskManager(db_config)
    new_disk_manager.LoadState()
    print(f"Loaded free pages: {new_disk_manager.free_pages}")

    # Cleanup after tests
    print("\nCleaning up test files...")
    cleanup_test_files(db_config)

def cleanup_test_files(db_config):
    if os.path.exists(f"./{db_config.dbpath}/dm.save"):
        os.remove(f"./{db_config.dbpath}/dm.save")
    if os.path.exists(f"./{db_config.dbpath}/BinData"):
        for file in os.listdir(f"./{db_config.dbpath}/BinData"):
            os.remove(f"./{db_config.dbpath}/BinData/{file}")
        os.rmdir(f"./{db_config.dbpath}/BinData")
    os.rmdir(f"./{db_config.dbpath}")

if __name__ == '__main__':
    manual_test_disk_manager()

