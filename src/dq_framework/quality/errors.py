"""Eccezioni del framework di Data Quality.

Sono errori *fatali*: non vengono catturati dagli entrypoint, così
`spark-submit` termina con exit code != 0 e CDE marca il job come FAILED.
Un contract non valido non deve mai produrre un run "verde" e silenzioso.
"""

from __future__ import annotations


class DQFrameworkError(Exception):
    """Base di tutti gli errori fatali del framework."""


class ContractError(DQFrameworkError):
    """Il Data Contract non è leggibile o non è utilizzabile per lo scan."""


class ContractNotReadableError(ContractError):
    """Il Data Contract non è stato scaricato/letto (path errato, 404, YAML rotto)."""


class ScanExecutionError(DQFrameworkError):
    """Lo scan Soda non ha prodotto esiti utilizzabili.

    Casi tipici: query di un check andata in errore (OOM/executor morto/SQL non
    valido), tabella sorgente non caricabile, zero check valutati. Senza questo
    errore il job uscirebbe con 0 pur non avendo scritto alcun esito: su CDE
    apparirebbe SUCCEEDED e la tabella results resterebbe semplicemente senza
    righe per quel run, che e' indistinguibile da "tutto ok" a colpo d'occhio.
    """


class ContractValidationError(ContractError):
    """Il Data Contract è leggibile ma non contiene una specifica di quality valida.

    Casi tipici: blocco 'quality' assente o di tipo diverso da 'SodaCL',
    'dataset' o 'specification' mancanti.
    """
