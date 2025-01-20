import struct
from pageId import PageId
from Record import Record
from RecordId import RecordId


class Relation:
    def __init__(self, relationName, nbCollumn, colInfoList, tailleVar, headerPageId, diskManager, bufferManager):
        self.relationName = relationName  # Nom de la relation
        self.nbCollumn = nbCollumn  # Nombre de colonnes
        self.col_info_list = colInfoList  # Liste des informations sur les colonnes
        self.tailleVar = tailleVar  # Taille variable ou fixe
        self.headerPageId = headerPageId  # PageId de la Header Page
        self.diskManager = diskManager  # Référence vers DiskManager
        self.bufferManager = bufferManager  # Référence vers BufferManager

    def writeRecordToBuffer(self, record, buff, pos):
        """
        Ecrit un record dans un buffer à partir de la position pos.
        Gère les formats à taille fixe et à taille variable.
        Retourne la taille totale écrite dans le buffer.
        """
        posRel = pos # Position relative dans le buffer
        index = 0  # Index des valeurs dans le record

        if self.tailleVar:  # Gestion de taille variable avec offset directory
            offset_directory = []
            start_offset = pos + (len(self.col_info_list)) * 4  # n+1 offsets au début
            current_offset = start_offset

            for col in self.col_info_list:
                offset_directory.append(current_offset - pos)  # Ajout de l'offset courant
                value = record.get_valeurs()[index]

                if col.colType == "INT":
                    buff[current_offset:current_offset ] = struct.pack('i', int(value))
                    current_offset += 4
                elif col.colType == "REAL":
                    buff[current_offset:current_offset + 4] = struct.pack('f', float(value))
                    current_offset += 4
                elif col.colType.startswith("CHAR") or col.colType.startswith("VARCHAR"):
                    for char in value:
                        buff[current_offset:current_offset + 1] = char.encode('utf-8')
                        current_offset += 1 
                index += 1

            offset_directory.append(current_offset - pos)  # Dernier offset : position de fin

            for i, offset in enumerate(offset_directory):  # Ecriture des offsets
                buff[pos + i * 4:pos + (i + 1) * 4] = struct.pack('i', offset)
            res = current_offset - pos  # Taille totale écrite

        else: 
             # Gestion à taille fixe
            for col in self.col_info_list:
                value = record.get_valeurs()[index]

                if col.colType == "INT":
                    buff[posRel:posRel+4] = struct.pack("i", int(value))
                    posRel += 4
                elif col.colType == "REAL":
                    buff[posRel:posRel + 4] = struct.pack("f", float(value))
                    posRel += 4
                elif col.colType.startswith("CHAR"):
                    taille = int(col.colType[5:-1])
                    value = value.ljust(taille, "$")[:taille]  # Tronquer ou remplir avec '$'
                    buff[posRel:posRel + taille] = value.encode("utf-8")
                    posRel += taille
                index += 1  
            res = posRel - pos  # Taille totale écrite
        return res

    def readFromBuffer(self, record, buff, pos):
        """
        Lit un record depuis un buffer à partir de la position pos.
        Remplit le record avec les valeurs lues et retourne la taille totale lue.
        """
        posRel = pos  # Position actuelle dans le buffer
        valeurs = []

        if not self.tailleVar:  # Gestion à taille fixe
            for col in self.col_info_list:
                if col.colType == "INT":
                    valeur = struct.unpack("i", buff[posRel:posRel + 4])[0]
                    posRel += 4
                elif col.colType == "REAL":
                    valeur = struct.unpack("f", buff[posRel:posRel + 4])[0]
                    posRel += 4
                elif col.colType.startswith("CHAR"):
                    taille = int(col.colType[5:-1])
                    valeur = buff[posRel:posRel + taille].decode("utf-8").rstrip("$")
                    posRel += taille
                valeurs.append(valeur)
            record.set_valeurs(valeurs)
            return posRel - pos

        else:  # Gestion à taille variable avec offset directory
            offsets = []
            nb_offsets = len(self.col_info_list) + 1
            for _ in range(nb_offsets):
                offset = struct.unpack("i", buff[posRel:posRel + 4])[0]
                offsets.append(offset)
                posRel += 4

            for i, col in enumerate(self.col_info_list):
                start = pos + offsets[i]
                end = pos + offsets[i + 1]
                if col.colType == "INT":
                    valeur = struct.unpack("i", buff[start:end])[0]
                elif col.colType == "REAL":
                    valeur = struct.unpack("f", buff[start:end])[0]
                elif col.colType.startswith("VARCHAR"):
                    valeur = buff[start:end].decode("utf-8")
                valeurs.append(valeur)

            record.set_valeurs(valeurs)
            return offsets[-1]  # Taille totale lue


    def addDataPage(self):
        """
        Rajoute une page de données vide au Heap File et met à jour le Page Directory.
        """
        # Allouer une nouvelle page via DiskManager
        new_page = self.diskManager.AllocPage()

        # Charger la Header Page dans le buffer
        header_page_idx = self.bufferManager.getPage(self.headerPageId)
        header_page = self.bufferManager.buffer_pool[header_page_idx]
        # Lire le nombre actuel de pages de données
        nb_pages = struct.unpack('i', header_page[:4])[0]

        # Mettre à jour les informations pour la nouvelle page dans le Page Directory
        offset = 4 + nb_pages * 12  # Chaque entrée fait 12 octets (2x4 pour PageId + 4 pour espace libre)
        header_page[offset:offset + 4] = struct.pack('i', new_page.FileIdx)
        header_page[offset + 4:offset + 8] = struct.pack('i', new_page.PageIdx)
        header_page[offset + 8:offset + 12] = struct.pack('i', self.bufferManager.db_config.get_pageSize() - 8)

        # Mettre à jour le nombre total de pages
        nb_pages += 1
        header_page[:4] = struct.pack('i', nb_pages)

        # Libérer la Header Page après modification
        headerTmp = self.bufferManager.getPage(self.headerPageId)
        self.bufferManager.buffer_pool[headerTmp][:4] = struct.pack("i",self.headerPageId.FileIdx)
        self.bufferManager.buffer_pool[headerTmp][4:8] = struct.pack("i",self.headerPageId.PageIdx)
        self.bufferManager.buffer_pool[headerTmp][20:] = header_page[20:] 
        self.bufferManager.FreePage(self.headerPageId, True)

        # Initialiser la nouvelle page de données dans le buffer
        new_page_idx = self.bufferManager.getPage(new_page)
        new_page_buffer = self.bufferManager.buffer_pool[new_page_idx]

        # Initialiser les deux entiers en fin de page : position libre et nombre d'entrées
        page_size = self.bufferManager.db_config.get_pageSize()
        for i in range(20,page_size+20):
            new_page_buffer[i:i+1] = struct.pack("c",b"0")
        new_page_buffer[page_size - 8:page_size - 4] = struct.pack('i', 0)  # Position libre = 0
        new_page_buffer[page_size - 4:] = struct.pack('i', 0)  # Nombre d'entrées = 0

        # Libérer la nouvelle page
        self.bufferManager.buffer_pool[self.bufferManager.getPage(new_page)] = new_page_buffer
        self.bufferManager.FreePage(new_page, valdirty=True)

    def getFreeDataPageId(self, sizeRecord):
        """
        Retourne le PageId d'une page de données avec suffisamment d'espace pour insérer le record.
        Si aucune page ne convient, retourne None.
        """
        # Charger la Header Page dans le buffer
        header_page_idx = self.bufferManager.getPage(self.headerPageId)
        header_page = self.bufferManager.buffer_pool[header_page_idx]
        # Lire le nombre de pages de données
        nb_pages = struct.unpack('i', header_page[20:24])[0]

        # Parcourir toutes les pages de données pour vérifier l'espace disponible
        if nb_pages >0:
            for i in range(nb_pages):
                offset = 4 + i * 12  # Offset de la case i dans le Page Directory
                file_idx = struct.unpack('i', header_page[offset:offset + 4])[0]
                page_idx = struct.unpack('i', header_page[offset + 4:offset + 8])[0]
                free_space = struct.unpack('i', header_page[offset + 8:offset + 12])[0]

                # Vérifier si la page a assez d'espace pour le record
                if sizeRecord + 8 <= free_space:  # Ajouter 8 pour l'entrée dans le slot directory
                    pageId = PageId(file_idx, page_idx)

                    # Libérer la Header Page avant de retourner
                    header_page[20:24] = struct.pack("i",nb_pages+1)
                    self.bufferManager.buffer_pool[self.bufferManager.getPage(self.headerPageId)] = header_page
                    self.bufferManager.FreePage(self.headerPageId, valdirty=False)
                    return pageId
        
            
        # Si aucune page n'a assez d'espace, libérer la Header Page et retourner None
        else:
            offset = 20
            tmp = self.bufferManager.disk_manager.AllocPage()
            self.bufferManager.buffer_pool[header_page_idx][offset:offset+4] = struct.pack("i", 1)
            self.bufferManager.buffer_pool[header_page_idx][offset+4:offset+8] = struct.pack("i", tmp.FileIdx)
            self.bufferManager.buffer_pool[header_page_idx][offset+8:offset+12] = struct.pack("i", tmp.PageIdx)
            self.bufferManager.buffer_pool[header_page_idx][offset+12:offset+16] = struct.pack("i", self.bufferManager.db_config.get_pageSize()-8)
            self.bufferManager.FreePage(self.headerPageId, True)   

            tmpIndex = self.bufferManager.getPage(tmp)     
            for i in range(20,self.bufferManager.db_config.get_pageSize()):
                self.bufferManager.buffer_pool[tmpIndex][i:i+1] = struct.pack("c", b"0")
            self.bufferManager.buffer_pool[tmpIndex][self.bufferManager.db_config.get_pageSize()-8:self.bufferManager.db_config.get_pageSize()-4] = struct.pack("i", 0)
            self.bufferManager.buffer_pool[tmpIndex][self.bufferManager.db_config.get_pageSize()-4:self.bufferManager.db_config.get_pageSize()] = struct.pack("i", 0)
            #self.bufferManager.buffer_pool[header_page_idx][offset+12:offset+16] = struct.pack("i", self.bufferManager.db_config.get_pageSize()-8)
            self.bufferManager.FreePage(tmp, True)      
            return tmp
    
    def writeRecordToDataPage(self, record, pageId):
        """
        Écrit un enregistrement dans la page de données identifiée par pageId.
        Retourne le RecordId de l'enregistrement écrit.
        """
        # Obtenir la taille de la page depuis la configuration
        page_size = self.diskManager.getDBC().get_pageSize() +20

        # Emprunter la page de données via le BufferManager
        page_idx = self.bufferManager.getPage(pageId)
        page = self.bufferManager.buffer_pool[page_idx]
        for i in range(20,page_size-8):
                page[i:i+1] = struct.pack("c",b"0")

        # Lire la position libre actuelle et le nombre de slots dans le Slot Directory
        position_libre = struct.unpack('i', page[page_size-4:page_size])[0] 
        nb_slots = struct.unpack('i', page[page_size-8:page_size-4])[0] 

        # Écrire le record dans le buffer à partir de la position libre
        taille_record = self.writeRecordToBuffer(record, page, position_libre + 20 )

        # Mettre à jour le Slot Directory
        slot_offset = page_size - 8 - (nb_slots * 8)
        struct.pack_into('i', page, slot_offset, position_libre)        # Position du record
        struct.pack_into('i', page, slot_offset + 4, taille_record)     # Taille du record

        # Mettre à jour le nombre de slots et la position libre
        nb_slots += 1
        position_libre += taille_record
        page[page_size - 8:page_size - 4] = struct.pack("i",nb_slots)            # Mettre à jour nb_slots
        page[page_size - 4:page_size] = struct.pack("i",position_libre)            # Mettre à jour nb_slots    # Mettre à jour position libre

        # Libérer la page avec valdirty=True pour indiquer qu'elle a été modifiée
        self.bufferManager.buffer_pool[page_idx][:4] = struct.pack("i",pageId.FileIdx)
        self.bufferManager.buffer_pool[page_idx][4:8] = struct.pack("i",pageId.PageIdx)
        self.bufferManager.buffer_pool[page_idx][20:] = page[20:]
        #self.bufferManager.disk_manager.WritePage(pageId,self.bufferManager.buffer_pool[page_idx])
        self.bufferManager.FreePage(pageId,True)

        # Retourner le RecordId correspondant
        return RecordId(pageId, nb_slots - 1)


    def getRecordsInDataPage(self, pageId):
        """
        Renvoie la liste des records stockés dans la page identifiée par pageId.
        """
        # Obtenir la taille de la page depuis la configuration
        page_size = self.diskManager.getDBC().get_pageSize()

        # Emprunter la page via le BufferManager
        page_idx = self.bufferManager.getPage(pageId)
        page = self.bufferManager.buffer_pool[page_idx]

        # Lire le nombre de slots dans le Slot Directory
        nb_slots = struct.unpack('i', page[page_size - 8:page_size - 4])[0]

        # Liste pour stocker les records
        liste_de_records = []

        # Parcourir chaque slot dans le Slot Directory
        slot_offset = page_size - 8 - (nb_slots * 8)
        for _ in range(nb_slots):
            # Lire la position et la taille du record
            position = struct.unpack('i', page[slot_offset:slot_offset + 4])[0]
            taille = struct.unpack('i', page[slot_offset + 4:slot_offset + 8])[0]
            slot_offset += 8

            # Ignorer les records marqués comme supprimés (taille == 0)
            if taille == 0:
                continue

            # Lire le record depuis la position
            record = Record([])
            self.readFromBuffer(record, page, position)
            liste_de_records.append(record)

        # Libérer la page après lecture
        self.bufferManager.FreePage(pageId, valdirty=False)

        # Retourner la liste des records
        return liste_de_records

    def getDataPages(self):
        """
        Renvoie la liste des PageIds des pages de données, tels qu’ils figurent dans la Header Page.
        """
        # Liste pour stocker les PageIds
        liste_pages = []

        # Emprunter la Header Page via le BufferManager
        header_page_idx = self.bufferManager.getPage(self.headerPageId)
        header_page = self.bufferManager.buffer_pool[header_page_idx]

        # Lire le nombre de pages de données depuis la Header Page
        nb_pages = struct.unpack('i', header_page[:4])[0]  # Le premier entier contient N (nombre de pages)

        # Parcourir chaque entrée dans le Page Directory
        for i in range(nb_pages):
            offset = 4 + i * 12  # Chaque case prend 12 octets (2x4 pour PageId et 1x4 pour l'espace disponible)
            file_idx = struct.unpack('i', header_page[offset:offset + 4])[0]
            page_idx = struct.unpack('i', header_page[offset + 4:offset + 8])[0]
            liste_pages.append(PageId(file_idx, page_idx))

        # Libérer la Header Page après lecture
        self.bufferManager.FreePage(self.headerPageId, valdirty=False)

        # Retourner la liste des PageIds
        return liste_pages
    
    def InsertRecord(self, record):
        """
        Insère un Record dans une page de données.
        Retourne le RecordId correspondant.
        """
        # Calculer la taille du record
        buffer_size = self.bufferManager.disk_manager.getDBC().get_pageSize()
        temp_buffer = bytearray(buffer_size+20)
        record_size = self.writeRecordToBuffer(record, temp_buffer, 20)

        # Trouver une page avec de l’espace disponible
        page_id = self.getFreeDataPageId(record_size)

        # Si aucune page n’est disponible, ajouter une nouvelle page
        if page_id is None:
            self.addDataPage()
            page_id = self.getFreeDataPageId(record_size)

        # Écrire le record dans la page de données et retourner le RecordId
        
        return self.writeRecordToDataPage(record, page_id)
    
    def GetAllRecords(self):
        """
        Renvoie une liste contenant tous les records de la relation.
        """
        # Initialiser la liste des records
        liste_de_records = []

        # Obtenir la liste des pages de données
        data_pages = self.getDataPages()

        # Parcourir chaque page et ajouter ses records à la liste
        for page_id in data_pages:
            records_in_page = self.getRecordsInDataPage(page_id)
            liste_de_records.extend(records_in_page)

        return liste_de_records


