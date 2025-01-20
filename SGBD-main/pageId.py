class PageId:
    
    def __init__(self, FileIdx, PageIdx ):
      self.FileIdx = FileIdx
      self.PageIdx = PageIdx

    def __str__(self):
        return f"FileIdx={self.FileIdx}, PageIdx={self.PageIdx}"
    
    def get_FileIdx(self):
        return self.FileIdx

    def set_FileIdx(self, FileIdx):
       self.FileIdx = FileIdx

    def get_PageIdx(self):
       return self.PageIdx
        
    def set_page_idx(self, PageIdx):
         self.PageIdx = PageIdx

    def to_dict(self):
        return {
            "FileIdx" : self.FileIdx,
            "PageIdx" : self.PageIdx
        }