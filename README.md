# Template for Java Spring Microservice project

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=TODO-set-your-id&metric=alert_status)](https://sonarcloud.io/dashboard?id=TODO-set-your-id)
[![Integration Tests](https://github.com/pagopa/<TODO-repo>/actions/workflows/ci_integration_test.yml/badge.svg?branch=main)](https://github.com/pagopa/<TODO-repo>/actions/workflows/ci_integration_test.yml)

TODO: add a description

TODO: generate a index with this tool: https://ecotrust-canada.github.io/markdown-toc/

TODO: resolve all the TODOs in this template

---

## Api Documentation 📖

See the [OpenApi 3 here.](https://editor.swagger.io/?url=https://raw.githubusercontent.com/pagopa/<TODO-repo>/main/openapi/openapi.json)

---

## Technology Stack

- Java 17
- Spring Boot 3
- Spring Web
- Hibernate
- JPA
- ...
- TODO

---

## Start Project Locally 🚀

### Prerequisites

- docker

### Run docker container

from `./docker` directory

`sh ./run_docker.sh local`

ℹ️ Note: for PagoPa ACR is required the login `az acr login -n <acr-name>`

---

## Develop Locally 💻

### Prerequisites

- git
- maven
- jdk-17

### Run the project

Start the springboot application with this command:

`mvn spring-boot:run -Dspring.profiles.active=local`

### Spring Profiles

- **local**: to develop locally.
- _default (no profile set)_: The application gets the properties from the environment (for Azure).

### Testing 🧪

#### Unit testing

To run the **Junit** tests:

`mvn clean verify`

#### Integration testing

From `./integration-test/src`

1. `yarn install`
2. `yarn test`

#### Performance testing

install [k6](https://k6.io/) and then from `./performance-test/src`

1. `k6 run --env VARS=local.environment.json --env TEST_TYPE=./test-types/load.json main_scenario.js`

---

## Contributors 👥

Made with ❤️ by PagoPa S.p.A.

### Maintainers

See `CODEOWNERS` file



# 📊 GPD Data Quality Local

Questo progetto fornisce un ambiente standardizzato per l'esecuzione di controlli di qualità dei dati utilizzando **Soda Core** e **Apache Spark (PySpark)**. È progettato per essere eseguito localmente tramite **Dev Containers**.

### 1. Generazione Dati (`mock_data_setup.py`)
Prima di testare la qualità, il sistema deve avere dei dati. Questo script:
* Inizializza una **Spark Session** locale.
* Legge i **Data Contracts** (file YAML) per capire lo schema atteso (nomi delle colonne, tipi di dati).
* Genera programmaticamente dati sintetici ("Mock Data") che rispettano quegli schemi.

### 2. Controllo Qualità e Reporting (`data_quality.py`)
* Si connette ai dati generati tramite Spark.
* Esegue una **Soda Scan** basata sui file di configurazione e sui contratti.
* **Controlli eseguiti:** Verifica l'assenza di valori nulli, l'univocità delle chiavi primarie, la correttezza dei formati e la conformità degli schemi.
* **Integrazione Cloud:** Se configurato con le API Key nel file `.env`, invia i risultati direttamente alla dashboard di **Soda Cloud**, permettendo il monitoraggio storico dei test.

---

## 🚀 Guida all'avvio rapido

### 1. Prerequisiti
* **Docker Desktop**
* **VS Code** con estensione **Dev Containers** installata.

### 2. Setup delle credenziali
Crea un file `.env` nella cartella root (verrà ignorato da Git):
env
SODA_HOST=cloud.soda.io
SODA_API_KEY=le_tue_chiavi_soda
SODA_API_SECRET=il_tuo_secret_soda


### 3. Apertura dell'Ambiente (Dev Container)
* Apri la cartella del progetto con VS Code.
* Clicca sul tasto verde in basso a sinistra (><) oppure premi F1 e digita: Dev Containers: Reopen in Container.
* Attendi il completamento della build. Quando vedrai il terminale con root@..., l'ambiente sarà pronto.

### 4. Utilizzo del Makefile
Una volta dentro il container, Esegui Pipeline Completa (Mock + Quality):
* make run-soda