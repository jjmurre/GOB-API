# Amsterdam Schema Utility

The schema utility can be used to create Amsterdam Schema definitions for GOB datasets.

The tool can create GraphQL queries to retrieve GOB data in Amsterdam Schema format.

Finally, the tool can generate a curl command to automate the download of GOB data in Amsterdam Schema format. 

## Requirements

GOB-API requirements are installed.
For details see the GOB-API readme local installation

## Usage

The schema utility is started using:

`python amschema.py`

### Options

#### schema catalog

Generates the Amsterdam Schema for the given catalog

**Example:**

To generate the Amsterdam Schema for the meetbouten catalog:

`python amschema.py schema meetbouten` 

#### query catalog collection

Generates the GraphQL Query for the given catalog and collection

**Example:**

To generate the GraphQL Query for the metingen collection in the meetbouten catalog:

`python amschema.py query meetbouten metingen` 

#### curl catalog collection

Generates the curl statement to get the dataset for the given catalog and collection

**Example:**

To generate the curl statement to get the dataset for the metingen collection in the meetbouten catalog:

From a local running GOB API:

`python amschema.py curl meetbouten metingen --path localhost:8141/gob/graphql/streaming/`

From the GOB API in the acceptance environment:

`python amschema.py curl meetbouten metingen --path https://acc.api.data.amsterdam.nl/gob/graphql/streaming/`

## Other useful tools

### Amsterdam Schema Tools

https://pypi.org/project/amsterdam-schema-tools/

`pip install amsterdam-schema-tools`

This tool can be used to validate a generated schema.

**Example:**

`python amschema.py schema meetbouten > meetbouten.schema.json`

`schema validate https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema meetbouten.schema.json`

### Online JSON Schema Validator

https://www.liquid-technologies.com/online-json-schema-validator

To validate the JSON Schema for a single ndjson line

**Example:**

JSON data to validate:

`python amschema.py curl meetbouten meetbouten --path localhost:8141/gob/graphql/streaming/ | bash | head -1`

JSON Schema:

`python amschema.py schema meetbouten meetbouten`

## Amsterdam Schema Remarks

- NULL values are not supported, for example "type": ["string", "null"] cannot be used
- Many References are not supported
- The documentaion is not actively maintained. 
Many hyperlinks refer to out-of-date documentation or not working observables
- There is no getting-started manual to quickly get up and running
- References are always by id. References like identification + volgnummer are not supported
- There is no test environment. Imports cannot be tested.
- The schema validation tool is rather basic. Error messages are somewhat vague.
- The schema does not allow to have additional fields in the dataset.
- The schema requires a schema string to be present in every line of the dataset; inclusion of non-data.

## Large collections
The generated GraphQL for large collections may result in a 504 Gateway Timeout, because there is simply too
much data to fetch and return in too little time.

Use cursor-based GraphQL pagination to fetch the data in smaller chunks. See
[GraphQL Pagination](https://graphql.org/learn/pagination/). The `cursor` field is already returned when using
the generated GraphQL query. All that is left is to add the `first` and `after` parameters.