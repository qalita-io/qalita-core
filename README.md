# QALITA Platform CLI Core

<div style="text-align:center;">
<img width="250px" height="auto" src="https://cloud.platform.qalita.io/logo.svg" style="max-width:250px;"/>
</div>

## Architecture d'accès aux sources de données

Le module core fournit une abstraction unifiée pour accéder à de multiples types de sources de données via la classe abstraite `DataSource` et ses sous-classes spécialisées.

### Types de sources supportées

- mysql
- postgresql
- sqlite
- mongodb
- oracle
- s3
- gcs
- azure_blob
- hdfs
- sftp
- http
- https
- file
- folder

### Utilisation

Pour instancier la bonne source, utilisez la factory :

```python
from qalita_core.data_source_opener import get_data_source

ds = get_data_source(source_config)
df = ds.get_data(table_or_query, pack_config)
```

Chaque sous-classe implémente la méthode `get_data` pour charger les données dans un DataFrame ou un format adapté.

### Ajout d'un nouveau type de source

Pour ajouter un nouveau type, créez une sous-classe de `DataSource` et implémentez la méthode `get_data`.

---

Pour plus de détails, voir le code source dans `qalita_core/data_source_opener.py`.

# Poetry

### 0. Dependencies installation : 

Packages uses QALITA Azure Devops Artifacts Feed as a Pypi Package Proxy, you need to configure authentication to use it :

```bash
poetry config http-basic.qalita qalita <your-pat>
```

## Ajouter une dépendance

> poetry add <dependency>

## Run un process

> poetry run <command>

## Choix de l'environnement

poetry env use python3.13 &&
poetry install &&
pip install --user poetry-plugin-export &&
poetry export -f requirements.txt --output requirements.txt --without-hashes &&
pip install -r requirements.txt

## Dans VSCode choix de l'intérpréteur

> Ctrl + Shift + P -> Python: Select Interpreter -> Poetry

## shell :

> poetry shell

# Debug :

> poetry shell
> pip install --editable .
