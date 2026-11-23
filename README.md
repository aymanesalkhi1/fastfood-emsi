# FastFood.ma — pipeline temps réel Spark / Kafka

Projet Écosystème Big Data 2, EMSI, 5ème année, 2026-2027.
Binôme : [Nada Helali](https://github.com/nadahelali2), [Aymane Salkhi](https://github.com/aymanesalkhi1).

FastFood.ma livre à Casablanca, Rabat, Marrakech et Tanger. Chaque changement d'état d'une commande (`CREATED`, `PICKED_UP`, `DELIVERED`, `CANCELLED`) est un événement JSON dans Kafka. Spark le traite en batch puis en flux. MinIO garde le Data Lake.

```
producer.py → Kafka (topic orders, 3 partitions)
            → Spark Structured Streaming
            → MinIO  bronze/  JSON brut
                     silver/  Parquet nettoyé, partitionné
                     gold/    Delta, KPI par ville et par fenêtre de 10 min
```

Le lancement du cluster est décrit dans [docs/GUIDE.md](docs/GUIDE.md).

## Lancer

Prérequis : Docker Desktop avec au moins 6 Go de RAM, Python 3.11 ou 3.12, Java 17 pour un essai local sans Docker.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make up
make history
make load-history
```

Jupyter : <http://localhost:8888> (jeton `fastfood`). Spark Master : <http://localhost:8080>. MinIO : <http://localhost:9001> (`minioadmin` / `minioadmin`).

Les notebooks `notebooks/01` à `06` suivent les six parties du sujet. Le code qu'ils exécutent est dans `src/fastfood/`.

Depuis la machine hôte, Kafka s'annonce sur le port **9094** :

```bash
make producer    # 20 événements/s, comme le sujet
make replay      # rejoue l'historique, pour remplir Delta sans attendre 20 minutes
```

## Ce qui est où

| Chemin | Rôle |
| --- | --- |
| `docker-compose.yml` | Kafka KRaft, Spark master + 2 workers, MinIO, Jupyter |
| `producer/producer.py` | `--mode file --n 50000` ou `--mode kafka --rate 20` |
| `data/restaurants.json` | référentiel du sujet, 200 restaurants |
| `src/fastfood/partie1.py` … `partie6.py` | une partie du sujet par fichier |
| `notebooks/` | le même code, une partie par notebook |
| `rapport/Rapport_FastFood_Project.pdf` | rapport du projet |

Les identifiants MinIO et le jeton Jupyter sont ceux d'un TP. Ne pas exposer ce cluster sur Internet.

## Producteur

`producer/producer.py` et `data/restaurants.json` sont les deux fichiers de l'archive du sujet, sans modification. Le script cherche `restaurants.json` à côté de lui et écrit `orders_history.jsonl` dans le dossier courant : `make history` lui passe donc `--restaurants data/restaurants.json` et `--output data/orders_history.jsonl`. Le mode fichier (`--seed 42`, 50 000 lignes, 7 jours à partir du 9 novembre 2026) est déterministe : `make batch` retrouve les tableaux du rapport. Environ 5 % des événements ont un `event_time` en retard, jusqu'à 15 minutes. Environ 2 % sont des doublons (`order_id` + `status`). Quelques `restaurant_id` sont absents du référentiel. Le rejeu Kafka (`make replay`) est un script à part, `scripts/replay_kafka.py` : le producteur du sujet n'a pas ce mode.

## Parties

1. Session sur `spark://spark-master:7077`, version, master, application, `defaultParallelism`.
2. RDD : `filter` DELIVERED, `reduceByKey` du chiffre d'affaires, `aggregateByKey` du panier, top 5, lineage.
3. DataFrame et SQL, jointure broadcast, UDF contre `when`, Parquet partitionné par ville, `explain`.
4. GraphFrames : degrés, PageRank, composantes connexes, motifs, `aggregateMessages`.
5. Kafka → console, bronze JSON, compteurs, watermark 10 minutes.
6. Dédoublonnage, jointure stream-static, Parquet silver, fenêtres tumbling / sliding / session, Delta gold, reprise par checkpoint.

Le rapport à rendre est `rapport/Rapport_FastFood_Project.pdf`.
