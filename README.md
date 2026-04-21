# pagopa-platform-data-quality-framework

Framework per la **Data Quality** della piattaforma PagoPa CDP.

Il progetto interagisce automaticamente con i **Data Contract** — file YAML che descrivono schema, semantica e regole di qualità attese per ogni tabella — 
Il framework estrae da essi i controlli di qualità, li converte in SodaCL e li esegue contro i dati reali, producendo risultati strutturati e integrandosi opzionalmente con la dashboard **Soda Cloud** per il monitoraggio storico e, eventualmente, scrivendo e rielaborando tali risultati su tabelle del Data Lake.

---

## Moduli

Il framework espone due entrypoint distinti, pensati per essere invocati in maniera indipendente (es. da un Airflow DAG su CDE):

### `run_quality` — Data Quality Pipeline

Riceve il percorso di un Data Contract (file locale o URL GitHub), lo converte in SodaCL tramite `datacontract-cli` ed esegue la scansione Soda sul DataFrame Spark corrispondente. I risultati vengono scritti come DataFrame strutturato e, se le credenziali Soda Cloud sono configurate, inviati alla dashboard per il monitoraggio nel tempo.

```
spark-submit dq_framework.whl --entrypoint run_quality \
  --contract-path /path/to/contracts/silver_table.yaml
```

### `run_observability` — KPI & Metriche *(in sviluppo)*

Calcola e scrive KPI aggregati sul layer Silver, separati logicamente dai controlli di qualità puntuale. Pensato per alimentare cruscotti di osservabilità operativa.

---

## Struttura del repository

```
pagopa-platform-data-quality-framework/
├── .devcontainer/          # Dev Container (Dockerfile + devcontainer.json)
├── .github/                # Pipeline CI/CD e template PR
├── dq_framework/
│   ├── src/
│   │   ├── common/         # Configurazione per ambiente, logging, utility Spark
│   │   ├── entrypoints/    # Entry point spark-submit: run_quality, run_observability
│   │   ├── quality/        # Engine quality: parsing contratti, esecuzione Soda, scrittura risultati
│   │   └── observability/  # Calcolo e scrittura KPI
│   └── tests/
│       ├── fixtures/
│       │   ├── contracts/  # Data Contract YAML di esempio
│       │   └── data/       # CSV mock per i test
│       ├── integration/
│       └── unit/
├── .env                    # Credenziali locali (ignorato da Git)
├── Makefile                # Task di sviluppo
└── pyproject.toml          # Metadati pacchetto e dipendenze
```

---

## Sviluppo locale

### Come funziona l'ambiente

Lo sviluppo locale avviene all'interno di un **Dev Container**, che garantisce un ambiente riproducibile con Python 3.11, Java (richiesto da PySpark) e tutte le dipendenze già installate. Le operazioni quotidiane sono automatizzate tramite **Makefile**.

Prima di eseguire i controlli di qualità, è necessario avere dei dati: lo script `mock_data_setup.py` inizializza una Spark Session locale, legge i Data Contract per ricavarne lo schema atteso e genera programmaticamente dati sintetici conformi a quello schema.

### Prerequisiti

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [VS Code](https://code.visualstudio.com/) con l'estensione [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

### 1. Configurazione delle credenziali

Crea il file `.env` nella root del progetto (è già incluso in `.gitignore`):

```env
GITHUB_TOKEN=<personal access token per scaricare i contratti da GitHub>
SODA_HOST=cloud.soda.io
SODA_API_KEY=<API key Soda Cloud>
SODA_API_SECRET=<API secret Soda Cloud>
```

> Le credenziali Soda Cloud sono opzionali: senza di esse i controlli vengono comunque eseguiti e i risultati loggati localmente, ma non inviati alla dashboard cloud.

### 2. Apertura del Dev Container

1. Apri la cartella del progetto in VS Code.
2. Clicca sull'icona `><` in basso a sinistra oppure premi `F1` → **Dev Containers: Reopen in Container**.
3. Attendi il completamento della build. Quando il terminale mostra il prompt `root@...`, l'ambiente è pronto.

Il container installa automaticamente le dipendenze da `requirements.txt` e il pacchetto in modalità editabile (`pip install -e .`): ogni modifica al codice sorgente è immediatamente attiva senza rebuild.

### 3. Utilizzo del Makefile

Una volta dentro il container, tutte le operazioni di sviluppo sono disponibili tramite `make`. Per vedere i target disponibili, apri direttamente il `Makefile` oppure esegui:

```bash
cat Makefile
```

I target coprono tipicamente:

- **Linting** — verifica statica del codice
- **Esecuzione locale** — genera i dati mock ed esegue la pipeline quality in ambiente `dev`
- **Esecuzione con integrazione GitHub** — come sopra, ma con configurazione `dev-github` per testare il download dei contratti via API GitHub

> Il `Makefile` è la fonte di verità per i comandi disponibili: aprirlo direttamente è il modo più affidabile per scoprire i target aggiornati.

---

## Maintainer

Made with ❤️ by PagoPa S.p.A.

See [`CODEOWNERS`](CODEOWNERS) for the list of maintainers.
