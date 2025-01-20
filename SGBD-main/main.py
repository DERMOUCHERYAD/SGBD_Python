#!/usr/bin/env python3

from DBConfig import DBConfig


if __name__ == "__main__":
    print("Mon application est lancée !")
    config = DBConfig.load_db_config('config.txt')
    print(config.get_dbpath())
    print(config.get_pageSize())
    print(config.get_dm_maxfilesize())
    

#T'as rien testé YOUCEFF t'as juste récupérer le chemin haha depuis un fichier.txt alors qu'on a chiisi deja un fichier.json pour la config !!!
#Je parle des erreurs et pas un simple tests ? 
