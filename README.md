# pagopa-platform-data-quality-framework

Framework di Data Quality per la piattaforma PagoPa CDP: esegue sui dati del Data Lake i controlli dichiarati nei Data Contract e ne archivia gli esiti su tabelle Iceberg.

## Overview

Un **Data Contract** è un file YAML che descrive una tabella silver e, nel blocco `quality`, ne porta con sé i controlli di qualità già scritti in **SodaCL**. Il framework legge il contratto (da file locale o da GitHub), esegue quei controlli con Soda su un DataFrame Spark che punta alla tabella, e scrive gli esiti in due tabelle Iceberg: un log granulare per esecuzione (`<dl_layer>_dqf_<dominio>_results`) e il dettaglio dei record scartati (`<dl_layer>_dqf_<dominio>_failed_records`). Se sono configurate le credenziali Soda Cloud, alla dashboard vengono inviate solo le metriche aggregate, mai i record.

Risolve un problema concreto: tenere le regole di qualità insieme al contratto del dato, eseguirle in modo ripetibile su CDE (Cloudera Data Engineering) sotto orchestrazione Airflow, e lasciare a valle uno storico interrogabile — con il dettaglio delle righe che non passano i controlli — senza far uscire dati sensibili dal perimetro on-prem.

Il pacchetto espone due entrypoint `spark-submit`: `run_quality` (la pipeline descritta sopra, operativa) e `run_observability` (calcolo di KPI aggregati, in sviluppo).

## Installazione

Lo sviluppo avviene dentro il **Dev Container** in `.devcontainer/`, che fissa un ambiente riproducibile (`python:3.11-slim` con JRE per PySpark, `pyspark 3.5.8`, `soda-core 3.5.6`). Servono [Docker Desktop](https://www.docker.com/products/docker-desktop/) e [VS Code](https://code.visualstudio.com/) con l'estensione [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).

1. Crea il file `.env` nella root (è in `.gitignore`). `GITHUB_TOKEN` serve per scaricare i contratti da GitHub (ambienti `dev-github`/`prod`); le variabili Soda sono opzionali — senza, i controlli girano comunque e gli esiti restano locali:

   ```env
   GITHUB_TOKEN=<personal access token con permesso Contents: Read>
   SODA_HOST=cloud.soda.io
   SODA_API_KEY=<opzionale>
   SODA_API_SECRET=<opzionale>
   ```

2. Apri la cartella in VS Code → `F1` → **Dev Containers: Reopen in Container**. Alla creazione il container installa le dipendenze e il pacchetto in modalità editabile (`pip install -r requirements.txt && pip install -e '.[dev]'`), così ogni modifica al sorgente è subito attiva senza rebuild.

I comandi che seguono vanno eseguiti dentro il container.

## Utilizzo

Esecuzione minima end-to-end in locale:

```bash
make run-dev
```

Il target genera dati sintetici (`tests/mock_data_setup.py` legge il contratto per derivarne lo schema e scrive un DataFrame conforme), poi lancia la pipeline in ambiente `dev` sul contratto locale `tests/fixtures/contracts/payment_position.yaml`. È equivalente a:

```bash
ENV=dev python -m dq_framework.entrypoints.run_quality --domain gpd --dl-layer silver --watermark-from 1970-01-01
```

`--domain` e `--dl-layer` sono obbligatori e insieme compongono il nome delle tabelle di output (qui `silver_dqf_gpd_results` e `silver_dqf_gpd_failed_records`); `--watermark-from 1970-01-01` fa partire i controlli incrementali dall'epoch. In `dev` la scrittura su DB è disattivata (`results_write_enabled=False`): gli esiti vengono solo loggati, con il riepilogo dei check a fine run. Per provare invece il download del contratto da GitHub c'è `make run-dev-github`.

Lint (come in CI) e test:

```bash
ruff check .
pytest
```

> Nota: il target `make lint` invoca ancora `flake8`, ma la CI usa `ruff check .` — per allinearti a ciò che verrà validato, linta con `ruff`.

Su CDE la forma di invocazione è quella di produzione — `launcher.py` come application file e il wheel come dipendenza, con `ENV` iniettata nel driver/executor Spark:

```bash
spark-submit launcher.py \
  --domain gpd \
  --dl-layer silver \
  --contract-path src/data/pagopa/gpd/silver/dc-gpd-payment_option.yaml \
  --repository owner/repo --ref main
```

Il deploy del wheel + launcher e la creazione/aggiornamento del job CDE sono automatizzati da `scripts/deploy.sh <env> <endpoint> <profile> [--run]`.

## Come funziona la pipeline

L'entrypoint chiama `run_pipeline`, un orchestratore sottile: coordina i moduli in sequenza, mentre la logica vive nelle foglie. Passo per passo:

1. **Lettura del contratto** — `contract_reader` fa solo I/O: risolve il path (locale se `--repository` è vuoto, altrimenti scarica il file via GitHub Contents API con decodifica base64). `contract_parser` fa solo trasformazione pura: estrae il `dataset`, deriva il `table_name` (la foglia del dataset, con `-` → `_`, perché diventa il nome di una temp view SQL) e prende il **SodaCL così com'è** dal blocco `quality.specification`, riallineando i nomi tabella alle temp view Spark.

2. **Risoluzione del watermark** — se il SodaCL contiene `${INCREMENTAL_CONDITIONS}`, per ogni check il placeholder viene sostituito con la finestra `colonna > wm_from AND colonna <= wm_to`. Il `wm_from` si risolve in cascata: override da CLI → ultimo watermark letto dalla tabella dei risultati → bootstrap all'epoch. Senza placeholder è un no-op.

3. **Estrazione delle failed-query** — i `failed-query-fields` (una chiave che Soda non conosce e che farebbe fallire l'engine) vengono tolti dallo YAML e messi da parte, *dopo* la sostituzione del watermark, così la query salvata ha già i timestamp risolti.

4. **Scan Soda** — la tabella e le eventuali xref sono registrate come temp view e si esegue lo scan. Le righe fallite vengono catturate in RAM da un `MemorySampler`, mentre verso Soda Cloud i sample sono forzati a zero.

5. **Elaborazione degli esiti** — ogni check diventa una riga conforme allo schema dei risultati; il `check_name` è parsato secondo la naming convention (`fld__cmp__id__not_null` → categoria `field-level`, dimensione `completeness`, colonna `id`). Le colonne watermark si valorizzano solo per i check incrementali.

6. **Scrittura** — gli esiti vanno in due tabelle Iceberg: `<dl_layer>_dqf_<dominio>_results` (log granulare, una riga per check per run) e `<dl_layer>_dqf_<dominio>_failed_records` (dettaglio dei record scartati, ricostruito ri-eseguendo le failed-query differite con la primary key passata da `--primary-keys`). In `dev` la scrittura è disattivata e resta tutto a log.

Il punto che lega il tutto: la tabella `results` è insieme output e memoria. Il passo 2 rilegge da lì il watermark dei run precedenti filtrando per esito; con la policy di default (`pass_only`) il watermark avanza solo sui run riusciti, quindi una finestra che fallisce viene riprocessata finché il check non torna verde.

## Configurazione

Tre livelli si sovrappongono, dal più stabile al più puntuale.

**Config per ambiente.** Un `AppConfig` immutabile (`common/config/base.py`) è scelto a runtime dalla variabile `ENV` (default `dev`) tramite un registro. Ogni ambiente fissa la sorgente del contratto, `data_source`/`soda_host`, il database dei risultati e i parametri incrementali. Le differenze pratiche tra ambienti:

| campo | `dev` | `dev-github` | `test` | `prod` |
|---|---|---|---|---|
| sorgente contratto | file locale | GitHub | GitHub | GitHub |
| `table_limit` | 50 | 0 | 0 | 0 |
| `results_write_enabled` | `False` | `False` | `True` | `True` |

`table_limit=0` lascia le view lazy così Iceberg può fare partition pruning/pushdown sui check incrementali; il 50 di `dev` materializza in cache una slice ridotta per velocità. `results_write_enabled=False` fa sì che in locale gli esiti restino solo a log. Gli altri campi dell'`AppConfig` governano la colonna watermark di default (`dl_event_tms`), la policy di avanzamento (`pass_only`/`executed`), il lookback per i late arrival, il numero di record falliti campionati per check e le primary key surrogate.

**Override da CLI (per singolo run).** Gli argomenti di `run_quality` hanno priorità sui default dell'ambiente: `--domain` e `--dl-layer` (obbligatori, insieme decidono il nome delle tabelle di output — `<dl_layer>_dqf_<dominio>_results` — e sono validati come identificatori SQL semplici perché finiscono interpolati nella FQN), `--contract-path`/`--repository`/`--ref`, `--watermark-column` e `--watermark-from` (che bypassa il lookup automatico), `--primary-keys`, `--xref-datasets`, e `--dag-id`/`--airflow-run-id` (che di default leggono le variabili d'ambiente di Airflow).

**Segreti.** Pattern *file-then-env* (`common/secrets.py`): prima il file montato da CDE sotto `/etc/dex/secrets/<cred>/<key>`, poi la variabile d'ambiente (dal `.env` in locale). Vale per `GITHUB_TOKEN`, `SODA_API_KEY` e `SODA_API_SECRET`.

## Perché è fatto così

### Il SodaCL vive nel Data Contract, non è generato

In origine il framework convertiva il contratto in SodaCL con `datacontract-cli`; quella dipendenza è stata poi rimossa (commit *"removed datacontract cli"*) e oggi il SodaCL è scritto a mano direttamente nel blocco `quality.specification` del contratto, da cui viene letto così com'è. Basta guardare un contratto reale per capire il perché: i controlli usano query SQL custom con campo `failed-query-fields`, il placeholder `${INCREMENTAL_CONDITIONS}`, join cross-tabella e una naming convention precisa (`fld__cmp__id__not_null`) da cui il framework ricava categoria, dimensione e colonna dell'esito. Sono costrutti che un convertitore generico non produrrebbe: tenere il SodaCL nel contratto dà controllo totale sui check, a costo di scriverli manualmente.

### Controlli incrementali guidati da un watermark

Le tabelle silver sono flussi CDC di grandi dimensioni (il contratto di esempio dichiara ~15M record/anno), quindi ri-scansionare l'intera tabella a ogni run sarebbe uno spreco. Dove un check contiene `${INCREMENTAL_CONDITIONS}`, il framework lo sostituisce con una finestra temporale `colonna > wm_from AND colonna <= wm_to`. Il `wm_from` per ciascun check si risolve in cascata: override da CLI (`--watermark-from`), altrimenti lookup dell'ultimo watermark sulla tabella dei risultati, altrimenti bootstrap dall'epoch. La *advance policy* di default (`pass_only`) fa avanzare il watermark solo sulle run andate a buon fine, così una finestra che fallisce viene riprocessata finché il check non torna verde. Due dettagli servono a non vanificare tutto questo: il literal timestamp viene allineato al tipo reale della colonna (`TIMESTAMP` vs `TIMESTAMP_NTZ`) e in produzione `table_limit=0`, entrambi per non disabilitare il partition pruning / pushdown di Iceberg — se il filtro incrementale non spinge fino allo storage, il framework leggerebbe comunque tutta la tabella.

### Due tabelle Iceberg con ruoli distinti

Gli esiti sono separati in due tabelle con scopi diversi: `<dl_layer>_dqf_<dominio>_results` è il log granulare (una riga per check per esecuzione, con valore misurato, soglie, conteggi ed esito) e alimenta report e analisi storiche; `<dl_layer>_dqf_<dominio>_failed_records` contiene il dettaglio operativo delle righe che hanno violato un controllo (chiave primaria + valore incriminato), per andare a vedere *quali* record sono sbagliati. La separazione non è solo ordine: come descritto sopra, la tabella dei risultati fa doppio servizio ed è anche lo store da cui i run incrementali rileggono il proprio watermark, cosa che ha senso tenere distinta dal dettaglio ad alto volume dei record scartati.

### Il dettaglio delle righe fallite resta on-prem

I dati trattati sono di pagamento e contengono PII (nei contratti alcuni campi sono marcati `pii: true`). Per questo le righe che falliscono un controllo vengono catturate in RAM da un sampler custom (`MemorySampler`) e persistite nella tabella Iceberg on-prem, mentre verso Soda Cloud il `samples_limit` è forzato a `0`: alla dashboard arrivano solo le metriche aggregate, mai un singolo record. La cattura locale e l'upload cloud sono due percorsi distinti e indipendenti, proprio per garantire che il dettaglio sensibile non lasci il perimetro.

### Lo stesso wheel gira identico in locale e su CDE

La configurazione è selezionata a runtime dalla variabile `ENV`, che pesca da un registro di `AppConfig` immutabili (`dev`, `dev-github`, `test`, `prod`): stesso codice, comportamento diverso per ambiente (contratto di default, `data_source`, cap sulle righe, scrittura su DB on/off). Anche i segreti seguono la stessa logica *file-then-env*: su CDE sono montati come file sotto `/etc/dex/secrets/`, in locale arrivano dal `.env` — il codice prova prima il file e poi la variabile d'ambiente, senza doversi accorgere di dove sta girando. Coerente con questo, il deployment: `launcher.py` è un semplice shim di tre righe (importa e chiama `main`) perché `spark-submit` su CDE vuole un file come application entry, mentre tutta la logica viaggia impacchettata nel wheel montato come `--py-file`. Un solo artefatto, zero modifiche al codice tra portatile e cluster.
