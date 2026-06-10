"""Logs applicatifs structurés pour le TP5."""

import json
from datetime import datetime, timezone


def log_evenement(niveau: str, message: str, **contexte) -> None:
    """Écrit un log applicatif lisible dans les logs Airflow."""
    entree = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "niveau": niveau,
        "message": message,
        **contexte,
    }
    print(f"[TP5][{niveau}] {message} | {json.dumps(contexte, ensure_ascii=False)}")
