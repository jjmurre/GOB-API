# Dump

The GOB-API supports dumping collections to csv or to another database.

## Example

To dump gebieden stadsdelen from the acceptance environment you can use:

### Dump to CSV

Dumps the collection in CSV format:
- field separation character: ";"
- quotation character: '"'

```
https://acc.api.data.amsterdam.nl/gob/dump/gebieden/stadsdelen/?format=csv
```

### Dump to SQL

Returns the SQL statements to:
- Create a schema
- Create a table
- Import the data from a csv file
for the given catalog and collection:

```
https://acc.api.data.amsterdam.nl/gob/dump/gebieden/stadsdelen/?format=sql
```

### Dump to db (POST)

Connects to a remote database and dumps the given collection to another database.

The differences with the dump-to-csv and dump-to-sql functionalities are:
- The process is optimised in terms of speed, disk and memory usage
- The associated relations for the given catalog-collection are also dumped

```
curl -H "Content-Type: application/json" -d @config.json -X POST https://acc.api.data.amsterdam.nl/gob/dump/gebieden/stadsdelen/
```

template for config.json:

```
{
    "db": {
        "drivername": "postgres",
        "database": "anydatabase",
        "username": "anyuser",
        "password": "anypassword",
        "host": "anyhost",
        "port": anyport
    }
}
```
