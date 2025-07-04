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
   pip install -e .
   ```

> L'installation en mode développement (`-e`) permet d'utiliser la commande `bibliotheque` et de modifier le code sans réinstallation.

> **Note :** Assurez-vous que l'environnement est correctement configuré avant de continuer.

### Configuration de la base PostgreSQL

1. Démarrez le conteneur PostgreSQL avec Docker :
   ```bash
   docker compose up -d
   ```

2. Vérifiez que le conteneur est en cours d'exécution :
   ```bash
   docker ps
   ```

3. Configurez les variables d'environnement dans un fichier [`.env`](.env) en vous basant sur l'exemple contenu dans le fichier [`.env.example`](.env.example).

### Télécharger et traiter les données

#### Utilisation de la commande `bibliotheque`

Après installation, la commande `bibliotheque` est disponible globalement et remplace `python main.py` :

> Si vous rencontrez des soucis avec la commande `bibliotheque`, il reste tout de même possible d'utiliser la commande `python main.py` à la place.

Le fichier [`main.py`](main.py) est le point d'entrée principal du projet et propose une interface en ligne de commande (CLI) pour exécuter chaque étape du pipeline séparément.  
Vous pouvez l'utiliser ainsi :

```bash
bibliotheque <commande> [options]
```
ou 

```bash
python main.py <commande> [options]
```

Exemples de commandes :
- Voir l'aide :
  ```bash
  bibliotheque --help
  ```
- Télécharger les fichiers :  
  ```bash
  bibliotheque download_files
  ```
- Créer les tables PostgreSQL:  
  ```bash
  bibliotheque create_tables --model BAAI/bge-m3
  ```
- Traiter toutes les données :  
  ```bash
  bibliotheque process_files --all --model BAAI/bge-m3
  ```
- Diviser une table en sous tables basés sur differents critères (cf: [`main.py`](main.py)) :
  ```bash
  bibliotheque split_table --source legi
  ```
- Exporter les tables PostgreSQL en fichier parquet:  
  ```bash
  bibliotheque export_tables --output data/parquet
  ```
- Téléverser les datasets en format parquet sur le repository Hugging Face:
  ```bash
  bibliotheque upload_dataset --input data/parquet/service_public.parquet --dataset-name service-public
  ```


Executez `bibliotheque --help` dans votre terminal pour voir toutes les options disponibles, ou consultez directement le code contenu dans [`main.py`](main.py).


#### Utilisation alternative avec `python main.py`

Si vous préférez utiliser directement le script Python, vous pouvez toujours utiliser :

```bash
python main.py <commande> [options]
```

Exemples :
```bash
python main.py download_files
python main.py create_tables --model BAAI/bge-m3
python main.py process_files --all --model BAAI/bge-m3
```
#### Utilisation du script [`update.sh`](update.sh)

Le script [`update.sh`](update.sh) permet d'executer l'ensemble du pipeline de traitement des données : téléchargement, création des tables, vectorisation et export.  
Pour l'exécuter, lancez la commande suivante depuis la racine du projet :

```bash
source update.sh
```

Ce script va :
- Attendre que la base PostgreSQL soit disponible,
- Télécharger les fichiers publics,
- Créer ou mettre à jour les tables nécessaires dans la base,
- Traiter et vectoriser les données,
- Exporter les tables au format Parquet.

### Structure du projet

- **[`main.py`](main.py)** : Point d'entrée principal pour exécuter le pipeline complet via un CLI.
- **[`pyproject.toml`](pyproject.toml)** : Configuration du projet Python et des dépendances.
- **[`download_and_processing/`](download_and_processing/)** : Contient les scripts pour télécharger et extraire les fichiers.
- **[`database/`](database/)** : Contient les scripts pour gérer la base de données (création de tables, insertion de données).
- **[`utils/`](utils/)** : Contient des fonctions utilitaires partagées entre les différents modules.
- **[`config/`](config/)** : Contient les scripts de configuration du projet.
- **[`logs/`](logs/)** : Contient les fichiers journaux pour suivre l'exécution des scripts.
- **[`scripts/`](scripts/)** : Contient l'ensemble des script shell executés soit périodiquement ou manuellement dans certains cas.
  - **[`scripts/update.sh`](scripts/update.sh)** : Script shell pour éxecuter l'ensemble du pipeline de traitement des données.
  - **[`scripts/periodic_update.sh`](scripts/periodic_update.sh)** : Script shell pour automatiser l'ensemble de la pipeline sur la machine virtuelle. Ce script est executé periodiquement par [`cron_config.txt`](cron_config.txt).
  - **[`scripts/backup.sh`](scripts/backup.sh)** : Script shell pour sauvegarder le volume de la base Pgvector (PostgreSQL) ainsi que certains fichiers de configurations. Ce script est executé periodiquement par [`cron_config.txt`](cron_config.txt).
  - **[`scripts/restore.sh`](scripts/restore.sh)** : Script shell pour restaurer le volume de la base Pgvector (PostgreSQL) ainsi que certains fichiers de configurations si nécessaire.