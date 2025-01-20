from pageId import PageId

class RecordId:
    def __init__(self, pageId, slotIdx):
        self.pageId = pageId  # PageId indiquant la page du record
        self.slotIdx = slotIdx  # Indice dans le slot directory

    def __repr__(self):
        return f"RecordId(pageId={self.pageId}, slotIdx={self.slotIdx})"
