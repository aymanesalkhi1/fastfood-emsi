"""Partie 4 — graphe client → restaurant avec GraphFrames."""

from __future__ import annotations

import sys

from pyspark.sql.functions import (
    abs as sqlabs,
    col,
    count,
    countDistinct,
    desc,
    first,
    lit,
    sum as sqlsum,
)

from .io import read_orders, read_restaurants
from .metrics import update_metrics
from .paths import lake
from .session import build_spark, stop
from .transforms import deduplicate, enrich

PREGEL_NOTE = (
    "PageRank et aggregateMessages suivent le modèle Pregel : à chaque superstep, "
    "un sommet envoie un message à ses voisins, les messages sont agrégés, puis le "
    "sommet met à jour son état. On s'arrête après un nombre d'itérations (ici 10) "
    "ou quand le changement passe sous un seuil. GraphX expose ça via l'API RDD "
    "(Scala, Graph[VD, ED]). GraphFrames expose les mêmes algos sur des DataFrames, "
    "donc Catalyst optimise les motifs et PySpark peut les appeler. C'est pour ça "
    "que la partie 4 est en GraphFrames."
)


def _graphframe_cls():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    entry = str(root / "third_party")
    sys.path[:] = [entry] + [path for path in sys.path if path != entry]
    for name in list(sys.modules):
        if name == "graphframes" or name.startswith("graphframes."):
            del sys.modules[name]
    from graphframes import GraphFrame

    return GraphFrame


def build_frames(spark):
    orders = deduplicate(enrich(read_orders(spark), read_restaurants(spark))).cache()
    delivered = orders.filter(col("status") == "DELIVERED").cache()
    delivered.count()

    clients = (
        delivered.select(
            col("customer_id").alias("id"),
            lit("client").alias("type"),
            col("city").alias("city"),
            lit(None).cast("string").alias("category"),
            lit(None).cast("string").alias("name"),
        )
        .dropDuplicates(["id"])
    )
    restaurants = read_restaurants(spark).select(
        col("restaurant_id").alias("id"),
        lit("restaurant").alias("type"),
        col("city"),
        col("category"),
        col("name"),
    )
    # Le producteur officiel émet quelques restaurant_id absents du référentiel.
    # Sans ce sommet, l'arête n'a pas de destination et GraphFrames refuse le graphe.
    known = restaurants.select(col("id").alias("known_id"))
    orphans = (
        delivered.join(known, delivered.restaurant_id == known.known_id, "left_anti")
        .select(
            col("restaurant_id").alias("id"),
            lit("restaurant").alias("type"),
            col("city"),
            lit(None).cast("string").alias("category"),
            lit(None).cast("string").alias("name"),
        )
        .dropDuplicates(["id"])
    )
    vertices = clients.unionByName(restaurants).unionByName(orphans)
    edges = delivered.groupBy(
        col("customer_id").alias("src"),
        col("restaurant_id").alias("dst"),
    ).agg(
        count("*").alias("orders"),
        sqlsum("amount_mad").alias("amount"),
    )
    return orders, vertices, edges, orphans


def recommend(edges, motifs, customer_id: str, limit: int = 3) -> list[dict]:
    visited = edges.filter(col("src") == customer_id).select(col("dst").alias("visited"))
    pairs = motifs.select(
        col("c.id").alias("customer_id"),
        col("r1.id").alias("known"),
        col("r2.id").alias("candidate"),
        col("r2.name").alias("candidate_name"),
        col("r2.category").alias("category"),
        col("r2.city").alias("city"),
    )
    support = pairs.groupBy("known", "candidate", "candidate_name", "category", "city").agg(
        countDistinct("customer_id").alias("support")
    )
    proposals = (
        support.join(visited, support.known == visited.visited)
        .join(visited.select(col("visited").alias("candidate")), "candidate", "left_anti")
        .select("candidate", "candidate_name", "category", "city", "support")
        .dropDuplicates(["candidate"])
        .orderBy(desc("support"))
        .limit(limit)
    )
    return [row.asDict() for row in proposals.collect()]


def run(spark) -> dict:
    GraphFrame = _graphframe_cls()
    from graphframes.lib import AggregateMessages as AM

    spark.catalog.clearCache()
    # Sur le cluster, /tmp n'est pas partagé : les workers lisent un Parquet vide.
    spark.sparkContext.setCheckpointDir(lake("checkpoints", "graph"))
    # Le plan itératif de connectedComponents explose sous AQE (chaîne d'explain).
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    _orders, vertices, edges, orphans = build_frames(spark)
    graph = GraphFrame(vertices, edges)
    graph.cache()

    vertex_count = graph.vertices.count()
    edge_count = graph.edges.count()
    orphan_count = orphans.count()
    print(f"Sommets : {vertex_count}    Arêtes : {edge_count}    Restaurants hors référentiel : {orphan_count}")

    top_in = (
        graph.inDegrees.join(graph.vertices, "id")
        .filter(col("type") == "restaurant")
        .orderBy(desc("inDegree"))
        .select("id", "name", "city", "category", "inDegree")
        .limit(10)
    )
    top_out = (
        graph.outDegrees.join(graph.vertices, "id")
        .filter(col("type") == "client")
        .orderBy(desc("outDegree"))
        .select("id", "city", "outDegree")
        .limit(10)
    )
    in_rows = [row.asDict() for row in top_in.collect()]
    out_rows = [row.asDict() for row in top_out.collect()]

    ranked = graph.pageRank(resetProbability=0.15, maxIter=10)
    page = (
        ranked.vertices.filter(col("type") == "restaurant")
        .orderBy(desc("pagerank"))
        .select("id", "name", "city", "category", sqlround_pagerank())
        .limit(10)
    )
    page_rows = [row.asDict() for row in page.collect()]

    components = graph.connectedComponents(checkpointInterval=1)
    sizes = components.groupBy("component").count()
    component_count = sizes.count()
    largest = sizes.orderBy(desc("count")).first()["count"]
    city_of = (
        components.filter(col("type") == "restaurant")
        .groupBy("component")
        .agg(first("city", ignorenulls=True).alias("city"))
    )
    sized = sizes.join(city_of, "component", "left").orderBy(desc("count")).limit(12)
    component_rows = [
        {"city": row["city"] or "—", "vertices": int(row["count"])} for row in sized.collect()
    ]

    motifs = graph.find("(c)-[e1]->(r1); (c)-[e2]->(r2)").filter(
        (col("r1.category") == col("r2.category"))
        & (col("r1.id") != col("r2.id"))
        & (col("c.type") == "client")
        & (col("r1.type") == "restaurant")
    )
    motif_sample = [
        row.asDict(recursive=True)
        for row in motifs.select(
            col("c.id").alias("client"),
            col("r1.name").alias("restaurant_1"),
            col("r2.name").alias("restaurant_2"),
            col("r1.category").alias("category"),
        )
        .limit(5)
        .collect()
    ]
    candidates = [
        row["customer_id"]
        for row in motifs.groupBy(col("c.id").alias("customer_id"))
        .count()
        .orderBy(desc("count"))
        .limit(20)
        .collect()
    ]
    example_customer = candidates[0]
    proposals: list[dict] = []
    for customer_id in candidates:
        proposals = recommend(edges, motifs, customer_id)
        if proposals:
            example_customer = customer_id
            break

    aggregated = graph.aggregateMessages(
        sqlsum(AM.msg).alias("ca_messages"),
        sendToDst=AM.edge["amount"],
    )
    check = edges.groupBy("dst").agg(sqlsum("amount").alias("ca_groupby"))
    compared = aggregated.join(check, aggregated.id == check.dst)
    mismatch = compared.filter(sqlabs(col("ca_messages") - col("ca_groupby")) > 0.05).count()
    revenue_sample = [
        row.asDict()
        for row in aggregated.orderBy(desc("ca_messages")).limit(5).collect()
    ]

    print("Top inDegree (clients distincts) :", in_rows)
    print("Top outDegree :", out_rows)
    print("Top PageRank :", page_rows)
    print(f"Composantes connexes : {component_count}, plus grande : {largest}")
    print("Motifs :", motif_sample)
    print(f"Recommandation pour {example_customer} :", proposals)
    print(f"aggregateMessages vs groupBy, écarts : {mismatch}")
    print(PREGEL_NOTE)

    payload = {
        "vertices": vertex_count,
        "edges": edge_count,
        "orphan_restaurants": int(orphan_count),
        "top_in_degree": in_rows,
        "top_out_degree": out_rows,
        "top_pagerank": page_rows,
        "components": int(component_count),
        "largest_component": int(largest),
        "component_sizes": component_rows,
        "motif_sample": motif_sample,
        "recommendation_for": example_customer,
        "recommendations": proposals,
        "aggregate_mismatch": int(mismatch),
        "revenue_by_messages": revenue_sample,
        "pregel": PREGEL_NOTE,
    }
    update_metrics({"partie4": payload})
    if mismatch:
        raise RuntimeError("aggregateMessages ne retrouve pas le groupBy")
    return payload


def sqlround_pagerank():
    from pyspark.sql.functions import round as sqlround

    return sqlround(col("pagerank"), 6).alias("pagerank")


def main() -> dict:
    spark = build_spark("fastfood-partie4-graph", graph=True)
    try:
        return run(spark)
    finally:
        stop(spark)


if __name__ == "__main__":
    main()
