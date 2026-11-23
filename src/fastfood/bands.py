"""Tranche de distance. Fonction pure, réutilisée par l'UDF et les tests."""


def distance_band_value(distance_km: float | None) -> str | None:
    """court < 2 km, moyen de 2 à 5 km inclus, long au-delà."""
    if distance_km is None:
        return None
    if distance_km < 2:
        return "court"
    if distance_km <= 5:
        return "moyen"
    return "long"
