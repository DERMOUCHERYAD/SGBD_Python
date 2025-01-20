# Projet_BDDA (Mini SGBD)

## Table des matières
- [Présentation](#présentation)
- [Installation](#installation)
- [Execution](#execution)
- [Utilisation](#utilisation)
- [Contributeurs](#contributeurs)

## Présentation 
Ce projet consiste à développer un mini Système de Gestion de Base de Données (SGBD) en Python. Un SGBD est un logiciel qui permet de gérer, manipuler et interroger des données de manière structurée. Dans ce cadre, ce projet vise à implémenter certaines fonctionnalités essentielles d'un SGBD, telles que :

- La gestion des relations et des schémas de données.
- La manipulation de tuples (insertion, suppression, modification).
- L'exécution d'opérations relationnelles telles que les sélections, les projections, et les jointures.

L'objectif principal est de comprendre les concepts fondamentaux des bases de données relationnelles tout en renforçant les compétences en programmation orientée objet et en manipulation des structures de données.

## Fonctionnalités
1. Commande [CREATE DATABASE](#commande-create-database) : La commande `CREATE DATABASE` permet la création d'une base de données.
2. Commande [CREATE TABLE](#commande-create-table) : La commande `CREATE TABLE` permet la création de nouvelle table (relation) dans la base de données.
3. Commande [SET DATABASE](#commande-set-database) : La commande `SET DATABASE` permet de choisir une base de données courante.
4. Commande [LIST TABLES](#commande-list-tables) : La commande `LIST TABLES` permet d'afficher toutes les tables dans la base de données.
5. Commande [LIST DATABASES](#commande-list-databases) : La commande `LIST DATABASES` permet l'affichage de toutes les bases de données.
6. Commande [DROP TABLE](#commande-drop-table) : La commande `DROP TABLE` permet la suppression d'une table dans la base de données.
7. Commande [DROP TABLES](#commande-drop-tables) : La commande `DROP TABLES` permet la suppression de toutes les tables dans la base de données.
8. Commande [DROP DATABASES](#commande-drop-databases) : La commande `DROP DATABASES` permet la suppression de toutes les bases de données.
9. Commande [DROP DTABASE](#commande-drop-database) : La commande `DROP DATABASE` permet la suppression d'une base de donnée.
10. Commande [INSERT INTO](#commande-insert-into) : La commande `INSERT INTO` permet l'insertion d'un tuple dans une table.
11. Commande [BULKINSERT INTO](#commande-bulkinsert-into) : La commande `BULKINSERT INTO` permet l'insertion en bloc d'un ensemble de tuples dans une table, à partir d'un fichier CSV.
12. Commande [SELECT](#commande-slect) : La commande `SELECT` récupère des données d'une table en fonction de certaines conditions.


## Installation
Pour installer et exécuter le mini SGBD suivez les étapes ci-dessous :

### Prérequis
- Git : Le système de contrôle de version Git est utilisé pour cloner le dépôt. Vous pouvez installer Git à partir de [le site web de Git](https://git-scm.com/downloads).
- Python : Si vous souhaitez utiliser le script Python, assurez-vous d'avoir Python installé. Pour télécharger [Python](https://www.python.org/downloads/).

### Téléchargement du code source
Clonez le dépôt GitHub du projet en utilisant la commande suivante dans votre terminal :

```bash
git clone https://github.com/Imad-Bchl/SGBD.git
```

## Execution
Pour exécuter le projet, utilisez le script Python pour compiler et exécuter le programme :

## Utilisation
Voici comment utiliser les commandes du Mini SGBD :

### Commande CREATE DATABASE
```SQL
CREATE DATABASE NomBDD
 ```
### Commande CREATE TABLE
```SQL
CREATE TABLE NomTable (NomCol_1:TypeCol_1,NomCol_2:TypeCol_2, … NomCol_NbCol:TypeCol_NbCol)
 ```
### Commande SET DATABASE
```SQL
SET DATABASE NomBDD
 ```
### Commande LIST TABLES
```SQL
LIST TABLES
 ```
### Commande LIST DATABASES
```SQL
LIST DATABASES
 ```
### Commande DROP TABLE
```SQL
DROP TABLE NomTable
 ```
### Commande DROP TABLES
```SQL
DROP TABLES 
 ```
### Commande DROP DATABASES
```SQL
DROP DATABASES 
 ```
### Commande DROP DATABASE
```SQL
DROP DATABASE
 ```
### Commande INSERT INTO
```SQL
INSERT INTO nomRelation VALUES (val1,val2, … ,valn)
 ```
### Commande BULKINSERT INTO
```SQL
BULKINSERT INTO nomRelation nomFichier.csv
 ```
### Commande SELECT
```SQL
SELECT aliasRel.colp1, aliasRel.colp2,…, aliasRel.colpm FROM nomRelation aliasRel
 ```
## Contributeurs
- [BOUCHELLAL Imad Eddine](https://github.com/Imad-Bchl)
- [DERMOUCHE Ryad](https://github.com/DERMOUCHERYAD)
- [MERADI Youcef](https://github.com/NyroY64)
- [CHIBANI Ramzy](https://github.com/DZ-Ramzy)
