import math
import os
from pageId import PageId

class DiskManager:
    def __init__(self, dbc):
      self.dbc=dbc
      self.free_pages = []  

    def getDBC(self):
        return self.dbc  # Retourne l'objet DBConfig
    
    def AllocPage(self):
        # self.LoadState()
        # # If there are any free pages, reuse one
        # if self.free_pages:
        #     free_page = self.free_pages.pop(0)
        #     self.SaveState()
        #     return free_page
        # else:
            try:
                data_dir = os.path.join(self.dbc.dbpath, "BinData")
                os.makedirs(data_dir, exist_ok=True)

                # Get a list of existing data files
                files = [f for f in os.listdir(data_dir)]
                if files:
                    # Extract file indices and find the last file index
                    last_file_idx = int(files[len(files)-1][1])
                    last_file_name = files[len(files)-1] 
                    last_file_path = os.path.join(data_dir, last_file_name)
    
                    # Calculate current number of pages in the last file
                    file_size = os.path.getsize(last_file_path)
                    current_pages_in_file = file_size // self.dbc.pageSize
                    
                    # Check if the last file can accommodate a new page
                    if current_pages_in_file < self.dbc.dm_maxfilesize:
                        with open(last_file_path, "ab") as file:
                            file.close()
                        # Return the new PageId
                        return PageId(last_file_idx, current_pages_in_file)
                    
                    else:
                        # Create a new data file
                        new_file_idx = last_file_idx + 1
                        new_file_name = f"F{new_file_idx}.rsdb"
                        new_file_path = os.path.join(data_dir, new_file_name)
                        
                        with open(new_file_path, "wb") as new_file:
                            new_file.close()
                        # Return the new PageId
                        return PageId(new_file_idx, 0)
                else:
                    # No files exist yet; start with file index 0
                    new_file_idx = 0        
                    new_file_name = f"F{new_file_idx}.rsdb"
                    new_file_path = os.path.join(data_dir, new_file_name)
                    with open(new_file_path, "wb") as new_file:
                        new_file.close()
                    # Return the new PageId
                    return PageId(new_file_idx, 0)
            except Exception as e:
                print(f"Error in AllocPage: {e}")
                return None
        
    def DeallocPage (self,PageId):
        if PageId not in self.free_pages:
            self.free_pages.append(PageId)
            self.SaveState()
            
    
    def ReadPage(self, pageID):
        try:
            with open(f"./{self.dbc.dbpath}/BinData/F{pageID.FileIdx}.rsdb", "rb") as fichier:
                fichier.seek(self.dbc.pageSize*pageID.PageIdx)
                data = fichier.read(self.dbc.get_pageSize())
                fichier.close()
                return data

        except Exception as openFail:
            print(f"erreur d'ouverture de fichier = {openFail}")
            
            
            
    def WritePage(self, pageID, buff):
        try:
            with open(f"./{self.dbc.dbpath}/BinData/F{pageID.FileIdx}.rsdb", "wb") as fichier:
                fichier.seek(self.dbc.pageSize*pageID.PageIdx)
                fichier.write(buff)
                fichier.close()
            
        except Exception as openFail:
            print(f"erreur d'ouverture de fichier = {openFail}")
            
            
            
         
           
    def SaveState(self):
        data_dir = os.path.join(self.dbc.dbpath, "./DB_test/dm.save")
        os.makedirs(data_dir, exist_ok=True)
        
        try:
            with open(f"./{self.dbc.dbpath}/dm.save", "wb") as fichier:
                for element in self.free_pages:
                    fichier.write(f"{element.FileIdx},{element.PageIdx}\n".encode())
        except Exception as openFail:
            print(f"Error opening file: {openFail}")
  
  
    def LoadState(self):
        try:
            with open(f"./{self.dbc.dbpath}/dm.save", "r") as fichier:
                contenu = fichier.readlines()
                self.free_pages = [PageId(int(ligne.split(',')[0]), int(ligne.split(',')[1])) for ligne in contenu]
        except FileNotFoundError:
            # Initialize free_pages if file doesn't exist
            self.free_pages = []
        except Exception as openFail:
            print(f"erreur d'ouverture de fichier = {openFail}")