# GOB-API

GOB API provides for a HAL JSON and GraphQL view on the GOB catalogs, collections and entities.

The root endpoint provides for all GOB catalogs.
Every endpoint contains the links to explore the data in more detail.

# Requirements

    * docker-compose >= 1.17
    * docker ce >= 18.03
    * python >= 3.6
    
# Installation

## Local

Make sure ANTLR4 is installed.

The name of the ANTLR-executable is system-dependent. Sometimes it is called
```antlr```, other times it is called ```antlr4```.

The build script default is ```antlr4```. To change this, run

    export ANTLR_CMD=<<any other value>>

Run

    cd src
    ./build.sh
    
to build and generate the project files.

Create a virtual environment:

    python3 -m venv venv
    source venv/bin/activate
    pip install -r src/requirements.txt
    
Or activate the previously created virtual environment

    source venv/bin/activate

The API depends on a running database.
To start a database instance follow the instructions in the GOB-Upload project.

Start the API

```
    cd src
    python -m gobapi
```

The API is exposed at:
- HAL JSON: http://127.0.0.1:8141/gob/
- GraphQL: http://localhost:8141/gob/graphql/

The IP address of the server is also reported at stdout when starting the API from the command line

### Streaming output

Instead of having the API compute the result and return it as a whole or in paged format,
data can also be retrieved streaming. This not only limits the memory usage of the API
but also allows for more easy processing of the data.

- Streaming HAL JSON output can be obtained by using ?streaming=true or ?ndjson=true as URL parameter.
- Streaming GraphQL output can be obtained by using the endpoint .../graphql/streaming

### Tests

```bash
    cd src
    sh test.sh
```

## Docker

```bash
    docker-compose build
    docker-compose up
```

The API is exposed at the same address as for the local installation.

### Tests

```bash
    docker-compose -f src/.jenkins/test/docker-compose.yml build
    docker-compose -f src/.jenkins/test/docker-compose.yml run --rm test
```
