# ALBERT Bibliothèque

## Description

Ce projet traite les données publiques mises à disposition par les administrations pour alimenter la base de connaissance d'ALBERT API et faciliter l'accès aux données publiques vectorisées dans la fonction publique.  
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
   pip install -r requirements.txt # Installe les dépendances
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

3. Configurez les variables d'environnement dans un fichier [`.env`](.env) en vous basant sur l'exemple contenu dans le fichier [`.env.example`](.env.example).

### Télécharger et traiter les données

#### Utilisation du script [`update_db.sh`](update_db.sh)

Le script [`update_db.sh`](update_db.sh) permet d'automatiser l'ensemble du pipeline de traitement des données : téléchargement, création des tables, vectorisation et export.  
Pour l'exécuter, lancez la commande suivante depuis la racine du projet :

```bash
source update_db.sh
```

Ce script va :
- Attendre que la base PostgreSQL soit disponible,
- Télécharger les fichiers publics,
- Créer ou mettre à jour les tables nécessaires dans la base,
- Traiter et vectoriser les données,
- Exporter les tables au format Parquet.

#### Utilisation de `main.py`

Le fichier [`main.py`](main.py) est le point d'entrée principal du projet et propose une interface en ligne de commande (CLI) pour exécuter chaque étape du pipeline séparément.  
Vous pouvez l'utiliser ainsi :

```bash
python main.py <commande> [options]
```

Exemples de commandes :
- Télécharger les fichiers :  
  ```bash
  python main.py download_files
  ```
- Créer les tables :  
  ```bash
  python main.py create_tables --model BAAI/bge-m3
  ```
- Traiter toutes les données :  
  ```bash
  python main.py process_files --all --model BAAI/bge-m3
  ```
- Exporter les tables :  
  ```bash
  python main.py export_tables --output data/parquet
  ```

Executez `python main.py --help` dans votre terminal pour voir toutes les options disponibles, ou consultez directement le code contenu dans [`main.py`](main.py).

### Structure du projet

- **[`download_and_processing/`](download_and_processing/)** : Contient les scripts pour télécharger et extraire les fichiers.
- **[`database/`](database/)** : Contient les scripts pour gérer la base de données (création de tables, insertion de données).
- **[`utils/`](utils/)** : Contient des fonctions utilitaires partagées entre les différents modules.
- **[`config/`](config/)** : Contient les scripts de configuration du projet.
- **[`logs/`](logs/)** : Contient les fichiers journaux pour suivre l'exécution des scripts.
- **[`main.py`](main.py)** : Point d'entrée principal pour exécuter le pipeline complet via un CLI.
- **[`update_db.sh`](update_db.sh)** : Script shell pour automatiser l'ensemble du pipeline de traitement des données.