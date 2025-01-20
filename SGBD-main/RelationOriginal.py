import struct

from pageId import PageId
from Record import Record
from RecordId import RecordId


class Relation:

    def __init__(self, relationName, nbCollumn, colInfoList, tailleVar, bufferManager, headerPageId):
        self.relationName = relationName
        self.nbCollumn = nbCollumn
        self.col_info_list = colInfoList
        self.tailleVar = tailleVar
        self.bufferManager = bufferManager
        self.headerPageId = headerPageId

    def getNbCollumn(self):
        return self.nbCollumn

    def getCol_info_list(self):
        return self.col_info_list

    def getBufferManager(self):
        return self.bufferManager

    def getHeaderPageId(self):
        return self.headerPageId

    def writeRecordToBuffer(self, record, buff, pos):
        posRel = pos  # Position actuelle dans le buffer
        index = 0  # Index des valeurs dans le record

        for col in self.col_info_list:
            col_type = col.get_colType()  # Type de colonne

            # INT : écrire 4 octets
            if col_type == "INT":
                buff[posRel:posRel + 4] = struct.pack("i", int(record.get_valeurs()[index]))
                posRel += 4
                index += 1

            # REAL (FLOAT) : écrire 4 octets
            elif col_type == "REAL":
                buff[posRel:posRel + 4] = struct.pack("f", float(record.get_valeurs()[index]))
                posRel += 4
                index += 1

            # CHAR(T) : chaîne de taille fixe T
            elif col_type.startswith("CHAR"):
                taille = int(col_type[5:-1])  # Extraire T
                valeur = record.get_valeurs()[index]
                if len(valeur) > taille:
                    valeur = valeur[:taille]  # Tronquer si la chaîne est trop longue
                else:
                    valeur = valeur.ljust(taille, "$")  # Compléter avec des '$' si nécessaire
                buff[posRel:posRel + taille] = valeur.encode("utf-8")
                posRel += taille
                index += 1

            # VARCHAR(T) : écrire d'abord la taille, puis la chaîne elle-même
            elif col_type.startswith("VARCHAR"):
                taille_max = int(col_type[8:-1])  # Extraire T
                valeur = record.get_valeurs()[index]
                taille_reelle = len(valeur)

                if taille_reelle > taille_max:
                    raise ValueError(f"VARCHAR size exceeded: {taille_reelle} > {taille_max}")

                # Écrire la taille (4 octets)
                buff[posRel:posRel + 4] = struct.pack("i", taille_reelle)
                posRel += 4

                # Écrire la chaîne
                buff[posRel:posRel + taille_reelle] = valeur.encode("utf-8")
                posRel += taille_reelle
                index += 1

        return posRel - pos  # Retourner la taille totale écrite




    def readFromBuffer(self, record, buff, pos):
           
        posRel = pos  # Position actuelle dans le buffer
        valeurs = []  # Liste pour stocker les valeurs lues

        for col in self.col_info_list:
            col_type = col.get_colType()  # Type de colonne

            # INT : lire 4 octets
            if col_type == "INT":
                valeur = struct.unpack("i", buff[posRel:posRel + 4])[0]
                valeurs.append(valeur)
                posRel += 4

            # REAL (FLOAT) : lire 4 octets
            elif col_type == "REAL":
                valeur = struct.unpack("f", buff[posRel:posRel + 4])[0]
                valeurs.append(valeur)
                posRel += 4

            # CHAR(T) : lire T octets
            elif col_type.startswith("CHAR"):
                taille = int(col_type[5:-1])  # Extraire T
                valeur = buff[posRel:posRel + taille].decode("utf-8").rstrip("$")
                valeurs.append(valeur)
                posRel += taille

            # VARCHAR(T) : lire la taille (4 octets), puis la chaîne
            elif col_type.startswith("VARCHAR"):
                taille_reelle = struct.unpack("i", buff[posRel:posRel + 4])[0]
                posRel += 4
                valeur = buff[posRel:posRel + taille_reelle].decode("utf-8")
                valeurs.append(valeur)
                posRel += taille_reelle

        record.set_valeurs(valeurs)  # Remplir les valeurs du record
        return posRel - pos  # Retourner la taille totale lue


    def addDataPage(self):
        bufferManager = self.bufferManager
        nb_octets_restant = bufferManager.disk_manager.getDBC().get_pageSize()
        nouvelle_page = bufferManager.disk_manager.AllocPage()

        headerPage = bufferManager.getPage(self.headerPageId)
        nb_pages = headerPage.read_int(0) or 0  # Si None, on initialise à 0
        nb_pages += 1
        headerPage[20:24] = struct.pack("i", 0)
        headerPage[24:28] = struct.pack("i", nb_pages)

        next_offset = 4 + (nb_pages - 1)*12

        # Écriture des informations sur la nouvelle page
        headerPage.write_int(next_offset, nouvelle_page.file_idx)
        headerPage.write_int(next_offset + 4, nouvelle_page.page_idx)
        headerPage.write_int(next_offset + 8, nb_octets_restant - 8)

        # Libération de la page d'en-tête
        bufferManager.Free_page(self.header_page_id, True)

        # Étape 4 : Préparation de la nouvelle page de données
        data_page = bufferManager.getPage(nouvelle_page)
        data_page.write_int(nb_octets_restant - 4, 0)
        data_page.write_int(nb_octets_restant - 8, 0)
        bufferManager.FreePage(nouvelle_page, flush=True)



    def getFreeDataPageId(self, sizeRecord):
    # Accéder à bufferManager pour obtenir la page d'en-tête
        bufferManager = self.bufferManager
        headerPage = bufferManager.getPage(self.headerPageId)

        # Lire le nombre de pages de données existantes
        nb_pages = headerPage.read_int(0) or 0  # Si None, on initialise à 0

        # Parcourir toutes les pages de données enregistrées
        for i in range(nb_pages):
            offset = 4 + i * 12  # Calcul de l'offset de la page

            # Lire l'espace restant dans la page
            available_space = headerPage.read_int(offset + 8)
            if available_space is None:
                continue

            # Vérifier si le record peut être inséré dans cette page
            if sizeRecord + 8 <= available_space:
                # Construire le PageId à partir des informations de la page
                file_idx = headerPage.read_int(offset)
                page_idx = headerPage.read_int(offset + 4)
                page = PageId(file_idx, page_idx)

                # Libérer la page d'en-tête
                bufferManager.FreePage(self.headerPageId,False)
                return page

        # Si aucune page ne convient, on libère la page d'en-tête et on retourne None
        bufferManager.FreePage(self.headerPageId,False)
        return None

    def writeRecordToDataPage(self, record, pageId):
        bufferManager = self.bufferManager
        page_size = bufferManager.getDiskManager().getDbConfig().getPageSize()

        # Emprunter la page de données
        page = bufferManager.getPage(pageId)

        # Lire la position libre sur la page
        position_libre = page.read_int(page_size - 4) or 0  # Si None, initialiser à 0

        # Écrire l'enregistrement dans la page
        taille_record = self.writeRecordToBuffer(record, page, position_libre)

        # Lire le nombre de records présents
        nb_slot = page.read_int(page_size - 8) or 0  # Si None, initialiser à 0

        # Mise à jour de la page (nombre de records et position libre)
        page.write_int(page_size - 8, nb_slot + 1)
        page.write_int(page_size - 4, position_libre + taille_record)

        # Calculer la taille du couple (position, taille)
        taille_pos = nb_slot * 8

        # Écrire le couple (position, taille) dans la page
        page.write_int(page_size - 8 - taille_pos - 8, position_libre)
        page.write_int(page_size - 8 - taille_pos - 4, taille_record)

        # Calculer la taille totale du record avec le couple
        taille_totale = taille_record + 8

        # Mettre à jour l'en-tête de la page dans le buffer
        headerPage = bufferManager.getPage(self.headerPageId)
        for i in range(headerPage.read_int(0) or 0):
            offset = 4 + i * 12
            file_idx = headerPage.read_int(offset)
            page_idx = headerPage.read_int(offset + 4)
            if file_idx == pageId.getFileIdx() and page_idx == pageId.getPageIdx():
                tmp = headerPage.read_int(offset + 8) or 0
                headerPage.write_int(offset + 8, tmp - taille_totale)

        # Libérer les pages (données et en-tête)
        bufferManager.FreePage(pageId, flush=True)
        bufferManager.FreePage(self.headerPageId, flush=True)

        # Effectuer un flush pour garantir que toutes les modifications sont écrites
        bufferManager.flushBuffers()

        # Retourner l'identifiant du record (RecordId)
        return RecordId(pageId, page_size - 8 - taille_pos - 8)

    def getRecordsInDataPage(self, pageId):
        bufferManager = self.bufferManager

        # Créer une liste pour stocker les records
        liste_de_records = []

        # Obtenir la taille de la page
        page_size = bufferManager.getDiskManager().getDbConfig().getPageSize()

        # Lire la page de données à partir du buffer
        page = bufferManager.getPage(pageId)

        # Lire le nombre d'enregistrements dans la page
        nb_record = page.read_int(page_size - 8) or 0

        pos = 0

        # Lire chaque record dans la page
        for _ in range(nb_record):
            record = Record([])  # Créer un record vide, à remplir après

            # Lire l'enregistrement depuis le buffer
            pos += self.readFromBuffer(record, page, pos)

            # Ajouter le record à la liste
            liste_de_records.append(record)

        # Libérer la page après lecture
        bufferManager.FreePage(pageId, flush=False)

        # Retourner la liste des records
        return liste_de_records

    def getDataPages(self):
        # Liste pour stocker les PageIds
        liste_pages = []

        # Accéder au BufferManager
        bufferManager = self.bufferManager

        # Lire la Header Page
        headerPage = bufferManager.getPage(self.headerPageId)

        # Obtenir le nombre de pages de données
        nb_pages = headerPage.read_int(0) or 0  # Si la valeur est None, on considère qu'il n'y a pas de pages

        # Parcourir chaque page pour récupérer les informations
        for i in range(nb_pages):
            # Calculer l'offset pour lire le file_idx et le page_idx
            offset = 4 + i * 12

            # Lire les indices du fichier et de la page
            file_idx = headerPage.read_int(offset)
            page_idx = headerPage.read_int(offset + 4)

            # Créer un nouvel objet PageId et l'ajouter à la liste
            if file_idx is not None and page_idx is not None:
                liste_pages.append(PageId(file_idx, page_idx))

        # Libérer la Header Page
        bufferManager.FreePage(self.headerPageId, flush=False)

        # Retourner la liste des PageIds
        return liste_pages

    def insertRecord(self, record):
        # Obtenir la taille d'une page depuis le gestionnaire de disque
        page_size = self.bufferManager.disk_manager.getDBC().get_pageSize()

        # Création d'un buffer temporaire pour calculer la taille du record
        byte_buffer = bytearray(page_size)
        buffer_record = memoryview(byte_buffer)  # Équivaut à un ByteBuffer en Python

        # Calculer la taille du record
        taille_record = self.writeRecordToBuffer(record, buffer_record, 0)

        # Trouver une page avec suffisamment d'espace pour insérer le record
        data_page = self.getFreeDataPageId(taille_record)
    
        # Si aucune page n'a assez d'espace, en créer une nouvelle
        if data_page is None:
            self.addDataPage()
            # Réessayer de trouver une page avec suffisamment d'espace
            data_page = self.getFreeDataPageId(taille_record)

        # Insérer le record dans la page de données et retourner son RecordId
        return self.writeRecordToDataPage(record, data_page)