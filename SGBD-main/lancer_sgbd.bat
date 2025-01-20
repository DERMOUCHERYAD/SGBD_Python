@REM # #!/bin/bash

@REM # MAIN_FILE="main.py"

@REM # if [ ! -f "$MAIN_FILE" ]; then
@REM #     echo "Erreur : le fichier $MAIN_FILE n'existe pas."
@REM #     exit 1
@REM # fi

@REM # # Exécution du programme Python
@REM # echo "Lancement de l'application..."
@REM # python3 "$MAIN_FILE"

@REM # # Vérifier si l'exécution s'est bien passée
@REM # if [ $? -eq 0 ]; then
@REM #     echo "Application lancée avec succès."
@REM # else
@REM #     echo "Erreur lors de l'exécution de l'application."
@REM #     exit 1
@REM # fi

@echo off

SET MAIN_FILE=SGBD.py

REM Vérifier si le fichier existe
IF NOT EXIST %MAIN_FILE% (
    echo Erreur : le fichier %MAIN_FILE% n'existe pas.
    EXIT /B 1
)

REM Exécuter le programme Python
echo Lancement de l'application...
python %MAIN_FILE%

REM Vérifier si l'exécution s'est bien passée
IF %ERRORLEVEL% EQU 0 (
    echo Application lancée avec succès.
) ELSE (
    echo Erreur lors de l'exécution de l'application.
    EXIT /B 1
)