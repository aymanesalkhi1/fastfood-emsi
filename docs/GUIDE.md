# Lancement

Docker Desktop, 6 Go de RAM. Python 3.11 ou 3.12 sur la machine hôte.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make up
make history
make load-history
```

| Service | Adresse |
| --- | --- |
| Spark Master | http://localhost:8080 |
| MinIO | http://localhost:9001 (`minioadmin` / `minioadmin`) |
| Jupyter | http://localhost:8888 (jeton `fastfood`) |

Depuis l'hôte, Kafka est sur le port **9094**.

```bash
make producer    # 20 événements/s
make replay      # rejoue l'historique dans Kafka
make batch       # parties 1 à 4 en local, sans le cluster
make groups      # kafka-consumer-groups.sh --describe
make lake        # mc ls --recursive local/lake
```

Les notebooks `notebooks/01` à `06` suivent les six parties. Le code exécuté est dans `src/fastfood/partie1.py` … `partie6.py`.

Les identifiants MinIO et le jeton Jupyter sont ceux d'un TP. Ne pas exposer ce cluster sur Internet.
