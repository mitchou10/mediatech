# Albert Data Legi

## Description

Ce projet traite les données publiques mises à disposition par la DILA (Direction de l'information légale et administrative) pour alimenter la base de connaissance d'ALBERT. Il inclut des scripts pour télécharger, traiter et insérer ces données dans une base PostgreSQL et base de donnée vectorielle.

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

- **`download_data/`** : Contient les scripts pour télécharger et extraire les fichiers.
- **`database/`** : Contient les scripts pour gérer la base de données (création de tables, insertion de données).
- **`logs/`** : Contient les fichiers journaux pour suivre l'exécution des scripts.
- **`main.py`** : Point d'entrée principal pour exécuter le pipeline complet.

