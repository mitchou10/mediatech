# ALBERT Bibliothèque
@TO UPDATE
## Description

Ce projet traite les données publiques mises à disposition par les administrations pour alimenter la base de connaissance d'ALBERT et de faciliter l'accès aux données publiques vectorisées dans la fonction publique. 
Il inclut des scripts pour télécharger, traiter, embedder, insérer ces données dans une base PostgreSQL et faciliter leur exportation via divers moyens.

## Instructions

### Installation des dépendances

1. Créez un environnement virtuel et activez-le :
   ```bash
   python -m venv .venv  # Créer l'environnement virtuel
   source .venv/bin/activate  # Activer l'environnement virtuel (Linux/Mac)
   ```

2. Installez les dépendances nécessaires :
   ```bash
   pip install -r requirements.txt
   ```

> **Note :** Assurez-vous que l'environnement est correctement configuré avant de continuer.

### Configuration de la base PostgreSQL

1. Démarrez le conteneur PostgreSQL avec Docker :
   ```bash
   docker-compose up -d
   ```

2. Vérifiez que le conteneur est en cours d'exécution :
   ```bash
   docker ps
   ```

3. Configurez les variables d'environnement dans un fichier `.env` :
   ```plaintext
   CNIL_DATA_FOLDER=data/cnil
   LEGI_DATA_FOLDER=data/legi
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DB=albert_data
   POSTGRES_USER=your_user
   POSTGRES_PASSWORD=your_password
   ```

### Télécharger et traiter les données

1. Exécutez le script pour télécharger les données publiques et mettre à jour la base de données :
   ```bash
   source update_db.sh
   ```

2. Les données téléchargées seront extraites, traitées et insérées dans la base PostgreSQL.

### Structure du projet

- **`download_and_processing/`** : Contient les scripts pour télécharger et extraire les fichiers.
- **`database/`** : Contient les scripts pour gérer la base de données (création de tables, insertion de données).
- **`utils/`** : Contient des fonctions utilitaires partagées entre les différents modules.
- **`config/`** : Contient les scripts de configuration du projet.
- **`logs/`** : Contient les fichiers journaux pour suivre l'exécution des scripts.
- **`main.py`** : Point d'entrée principal pour exécuter le pipeline complet.

