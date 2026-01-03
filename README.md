# MEDIATECH

[![License](https://img.shields.io/github/license/etalab-ia/mediatech?label=licence&color=red)](https://github.com/etalab-ia/mediatech/blob/main/LICENSE)
[![Hugging Face collection](https://img.shields.io/badge/🤗-Hugging%20Face%20collection-yellow)](https://huggingface.co/collections/hulk10/mediatech)


## 📝 Description

MEDIATECH automatise la collecte, l'extraction et la publication de jeux de données publics français sur Hugging Face. Ce projet facilite l'accès aux données administratives françaises pour les applications d'IA en maintenant à jour la collection [hulk10/mediatech](https://huggingface.co/collections/hulk10/mediatech).

## 🎯 Objectif

Mettre à jour automatiquement les datasets de la collection [hulk10/mediatech](https://huggingface.co/collections/hulk10/mediatech) à partir des sources officielles françaises (DILA, data.gouv.fr, etc.).

## � Installation

### 1. Installer les dépendances

```bash
make install
```

Ou manuellement avec `uv` :

```bash
uv sync --all-groups --dev
```

### 2. Configuration

Assure-toi d'avoir configuré ton token Hugging Face pour l'upload :

```bash
huggingface-cli login
```

## 📖 Usage

### Pipeline complet

Le pipeline se compose de deux étapes principales :

#### 1. Téléchargement des données

```bash
PYTHONPATH=./ uv run scripts/download.py --download_name <dataset> [options]
```

**Options disponibles :**
- `--download_name` : Nom du dataset à télécharger (voir [config/data_config.json](config/data_config.json))
- `--start_date` : Date de début au format YYYY-MM-DD (défaut: 2025-10-16)
- `--end_date` : Date de fin au format YYYY-MM-DD (défaut: aujourd'hui)
- `--max_download` : Nombre maximum de fichiers (-1 pour illimité)

**Exemples :**

```bash
# Télécharger LEGI depuis 2021
PYTHONPATH=./ uv run scripts/download.py --download_name legi --start_date 2021-01-01

# Télécharger CNIL pour la dernière semaine
PYTHONPATH=./ uv run scripts/download.py --download_name cnil --start_date 2025-12-25

# Télécharger DOLE
PYTHONPATH=./ uv run scripts/download.py --download_name dole
```

#### 2. Extraction et export vers Hugging Face

```bash
PYTHONPATH=./ uv run scripts/extraction_and_export.py --download_name <dataset> --user-id hulk10 [options]
```

**Options disponibles :**
- `--download_name` : Nom du dataset à extraire
- `--user-id` : ID utilisateur Hugging Face (hulk10)
- `--start_date` : Date de début (défaut: 2025-10-16)
- `--end_date` : Date de fin (défaut: aujourd'hui)

**Exemples :**

```bash
# Extraire et publier LEGI
PYTHONPATH=./ uv run scripts/extraction_and_export.py --download_name legi --user-id hulk10

# Extraire CNIL pour une période spécifique
PYTHONPATH=./ uv run scripts/extraction_and_export.py \
  --download_name cnil \
  --user-id hulk10 \
  --start_date 2025-01-01 \
  --end_date 2025-12-31
```

### Datasets disponibles

Les datasets configurés dans [config/data_config.json](config/data_config.json) incluent :

- **legi** : Législation française (DILA)
- **cnil** : Décisions de la CNIL
- **constit** : Décisions du Conseil Constitutionnel
- **dole** : Décisions du Journal Officiel
- **service_public_pro** : Fiches Service-Public.fr Pro
- **service_public_part** : Fiches Service-Public.fr Particuliers
- **state_administrations_directory** : Annuaire des administrations d'État
- **local_administrations_directory** : Annuaire des administrations locales
- **data_gouv_datasets_catalog** : Catalogue des datasets data.gouv.fr
- **travail_emploi** : Fiches Travail-Emploi

## 🔄 Workflow type

```bash
# 1. Télécharger les données
PYTHONPATH=./ uv run scripts/download.py --download_name legi --start_date 2024-01-01

# 2. Extraire et publier sur Hugging Face
PYTHONPATH=./ uv run scripts/extraction_and_export.py --download_name legi --user-id hulk10
```

Les données sont automatiquement :
1. Téléchargées depuis les sources officielles
2. Extraites et traitées
3. Partitionnées en Parquet
4. Publiées sur [https://huggingface.co/hulk10](https://huggingface.co/hulk10)

## � Structure du projet

```
mediatech/
├── scripts/
│   ├── download.py                    # Téléchargement des archives
│   └── extraction_and_export.py       # Extraction et export vers HF
├── src/
│   ├── download/                      # Modules de téléchargement
│   ├── extraction/                    # Modules d'extraction
│   ├── exports/                       # Modules d'export
│   ├── process/                       # Modules de traitement
│   └── utils/                         # Utilitaires
├── config/
│   └── data_config.json              # Configuration des sources
├── data/
│   ├── unprocessed/                  # Données brutes téléchargées
│   ├── extracted/                    # Données extraites
│   └── {dataset}/data/               # Parquets partitionnés
├── Makefile                          # Commandes utilitaires
└── pyproject.toml                    # Configuration Python

## ⚖️ License

Thi🛠️ Développement

### Commandes utiles

```bash
# Installer les dépendances
make install

# Lancer les tests
make run-test

# Linter le code
make lint

# Nettoyer les caches
make clean
```

### Ajouter un nouveau dataset

1. Ajoute la configuration dans [config/data_config.json](config/data_config.json)
2. Crée les modules de téléchargement, extraction et export nécessaires dans `src/`
3. Teste avec les scripts

## 🤗 Collection Hugging Face

Les datasets sont publiés dans la collection :  
**[https://huggingface.co/collections/hulk10/mediatech](https://huggingface.co/collections/hulk10/mediatech)**

Chaque dataset est disponible au format Parquet partitionné par fichier source, facilitant l'accès incrémental et la mise à jour.

## ⚖️ Licence

Ce projet est sous licence [MIT License](./LICENSE).

---

**Maintenu par** : [hulk10](https://huggingface.co/hulk10)  
**Collection** : [hulk10/mediatech](https://huggingface.co/collections/hulk10/mediatech)