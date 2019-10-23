# API

The GOB-API offers access to GOB collections.

## Example

To access gebieden stadsdelen from the acceptance environment you can use:

### REST
```
https://acc.api.data.amsterdam.nl/gob/gebieden/stadsdelen/
```

### REST streaming
```
https://acc.api.data.amsterdam.nl/gob/gebieden/stadsdelen/?stream=true
```

### ndjson
```
https://acc.api.data.amsterdam.nl/gob/gebieden/stadsdelen/?ndjson=true
```

### GraphQL
```
https://acc.api.data.amsterdam.nl/gob/graphql/?query=query%20%7B%0A%20%20gebiedenStadsdelen%20%7B%0A%20%20%20%20edges%20%7B%0A%20%20%20%20%20%20node%20%7B%0A%20%20%20%20%20%20%20%20naam%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D
```

### Related documentation

- [Functionality to dump GOB data to csv or another database](https://github.com/Amsterdam/GOB-API/blob/master/gobapi/dump/README.md)
