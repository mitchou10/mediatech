# Albert Data Legi

## Instructions

### Installer les modules necessaires

```bash
python -m venv .venv  # Créer l'environnement virtuel
source .venv/bin/activate  # Activer l'environnement virtuel (Linux/Mac)
pip install -r requirements.txt  # Installer les modules necessaires
```

> Assurez-vous que l'environnement est correctement configuré.


### Configuration de la base PostgreSQL

1. Start the PostgreSQL container using Docker:
   ```bash
   docker-compose up -d
   ```

2. Verify the container is running:
   ```bash
   docker ps
   ```
### Télécharger les données et créer la base de données SQL en local

Exécutez le script `update_db.sh` :

```bash
source update_db.sh
```

