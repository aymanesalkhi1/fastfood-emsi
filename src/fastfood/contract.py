"""Contrat de données FastFood.ma, partagé par le producteur et Spark.

Le producteur reste en bibliothèque standard : ce module n'importe pas PySpark.
"""

EVENT_FIELDS = (
    "order_id",
    "restaurant_id",
    "customer_id",
    "city",
    "status",
    "amount_mad",
    "items_count",
    "distance_km",
    "courier_id",
    "event_time",
)

STATUSES = ("CREATED", "PICKED_UP", "DELIVERED", "CANCELLED")
CITIES = ("Casablanca", "Rabat", "Marrakech", "Tanger")

# Spark lit le suffixe Z comme un caractère littéral. Le producteur écrit toujours en UTC.
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
ISO_PATTERN = "%Y-%m-%dT%H:%M:%SZ"

TOPIC_ORDERS = "orders"
TOPIC_PARTITIONS = 3
