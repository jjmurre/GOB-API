# GOB-API

GOB API provides for a HAL JSON view on the GOB catalogs, collections and entities.

The root endpoint provides for all GOB catalogs.
Every endpoint contains the links to explore the data in more detail.

# Requirements

    * docker-compose >= 1.17
    * docker ce >= 18.03
    * python >= 3.6
    
# Installation

## Local

Create a virtual environment:

    python3 -m venv venv
    source venv/bin/activate
    pip install -r src/requirements.txt
    
Or activate the previously created virtual environment

    source venv/bin/activate

The API depends on a running database.
To start a database instance follow the instructions in the GOB-Upload project.

If GOB-Upload project has already been initialised then execute:

```bash
    cd ../GOB-Upload
    docker-compose up database &
```

Start the API

```
    cd src
    python -m api
```

The API is exposed at http://127.0.0.1:5000/

### Tests

Linting
```bash
    cd src
    flake8
```

Unit tests
```bash
    cd src
    pytest
```

Test coverage (100%)
```bash
    cd src
    export COVERAGE_FILE=/tmp/.coverage
    pytest --cov=api --cov-fail-under=100 tests/
```

## Docker

```bash
    docker-compose build
    docker-compose up
```

### Tests

```bash
    docker-compose -f src/.jenkins/test/docker-compose.yml build
    docker-compose -f src/.jenkins/test/docker-compose.yml run --rm test
```
