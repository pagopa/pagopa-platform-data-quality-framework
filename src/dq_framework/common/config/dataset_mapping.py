"""
Configurazione delle chiavi primarie dei dataset.
Supporta chiavi primarie composite e campi annidati (es. 'after.id').
"""

DATASET_PK_MAP = {
    "silver_gpd_payment_position": ["after.id"],
}