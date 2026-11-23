PYTHON ?= python3
export PYTHONPATH := $(CURDIR)/src:$(CURDIR)/third_party
export JAVA_HOME ?= /usr/lib/jvm/java-17-openjdk-amd64
export PATH := $(JAVA_HOME)/bin:$(PATH)
export SPARK_MASTER ?= local[*]
export LAKE_URI ?= file:///tmp/fastfood-lake
export SPARK_UI_ENABLED ?= false

.PHONY: help restaurants history test batch up down load-history producer replay groups lake

help:
	@echo "make restaurants   référentiel 200 restaurants"
	@echo "make history       orders_history.jsonl (50 000 lignes)"
	@echo "make test          tests locaux, sans Docker"
	@echo "make batch         parties 1 à 4 en local"
	@echo "make up            cluster Kafka + Spark + MinIO + Jupyter"
	@echo "make load-history  copie l'historique dans le bucket lake"
	@echo "make producer      flux live, 20 événements/s, vers localhost:9094"
	@echo "make replay        rejoue l'historique dans Kafka (démo exactly-once)"
	@echo "make groups        kafka-consumer-groups.sh --describe"
	@echo "make lake          mc ls --recursive local/lake"

restaurants:
	@test -s data/restaurants.json

history: restaurants
	$(PYTHON) producer/producer.py --mode file --n 50000 --seed 42 \
		--restaurants data/restaurants.json \
		--output data/orders_history.jsonl
	$(PYTHON) scripts/late_example.py data/orders_history.jsonl

test:
	$(PYTHON) -m pytest

batch: history
	SPARK_MASTER=local[*] LAKE_URI=file:///tmp/fastfood-lake $(PYTHON) jobs/run_batch.py

up:
	docker compose up -d --build

down:
	docker compose down

load-history: history
	docker compose --profile tools run --rm --entrypoint /bin/sh mc -c '\
	  mc alias set local http://minio:9000 minioadmin minioadmin && \
	  mc mb -p local/lake || true && \
	  mc cp /data/orders_history.jsonl local/lake/bronze/history/orders_history.jsonl && \
	  mc cp /data/restaurants.json local/lake/bronze/history/restaurants.json && \
	  mc ls --recursive local/lake'

producer:
	$(PYTHON) producer/producer.py --mode kafka --rate 20 \
		--restaurants data/restaurants.json \
		--bootstrap localhost:9094

replay: history
	$(PYTHON) scripts/replay_kafka.py --file data/orders_history.jsonl --rate 400 --bootstrap localhost:9094

groups:
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups

lake:
	docker compose --profile tools run --rm --entrypoint /bin/sh mc -c '\
	  mc alias set local http://minio:9000 minioadmin minioadmin && \
	  mc ls --recursive local/lake'
