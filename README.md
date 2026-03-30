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
