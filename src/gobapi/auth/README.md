# Authorization

Authorization is implemented on both collection and attribute level.

### Keycloak, gatekeeper
The access is checked by means of required (keycloak) user-roles.

### Public and secure routes, use of headers
Routes.py controls the public and secure routes that exist in GOB-API.  
Secure routes use the keycloak headers to retrieve user-roles and be able to control access.  
The use of keycloak headers in public routes is (of course) forbidden and wil result in a 403 response.

### Authority logic
Auth_query.py implements the authorisation logic.  
It also defines a subclass of sqlalchemy.orm.Query that transparantly checks queries for access to protected data.

### Authorization scheme(s)
A GOB authorization scheme is defined in schemes.py.

Note:
- Multiple authorization schemes may exist.  
  The Authority class can work with any authorization scheme.

Example:

```
    "meetbouten": {
      "collections": {
          "meetbouten": {
              "attributes": {
                  "status": {
                      "roles": ["x"]
                  }
              }
          }
      }
    },
    "gebieden": {
        "collections": {
            "buurten": {
                "roles": ["x"]
            },
            "wijken": {
                "attributes": {
                    "ligt_in_stadsdeel": {
                        "roles": ["x"]
                    }
                }
            },
            "stadsdelen": {
                "attributes": {
                    "documentnummer": {
                        "roles": ["x"]
                    }
                }
            }
        }
    },


```

The required role in the above example is "x".

Without having role "x" a user cannot access:

- the status attribute of the meetbouten collection in the meetbouten catalog
- the buurten collection in the gebieden catalog
- the relation between wijken en stadsdelen in the wijken collection
- the documentnummer in the stadsdelen collection

# Testing

The authorization scheme has been intensively tested. The tests will become part of the end-2-end tests on short terms.

Testing involves testing of all API endpoints:

- REST
  - collection
  - single entity
- REST streaming
  - collection (stream=true)
- REST ndjson
  - collection (ndjson=true)
- ![red](https://placehold.it/15x15/f03c15/f03c15?text=+) REST view  
  This basically bypasses all authorisation as it simply exposes the result of a predefined SQL query.
  - (view=<name-of-view>)
- GraphQL
  - query
  - query with relations
  - query with inverse relations
  - aliases for collection
  - aliases for attributes
- GraphQL streaming  
  Requires postman (or curl) to issue a POST request with a query parameter
  - query
  - query with relations
- Dump csv
  - collection (format=csv)
- Dump sql
  - collection (format=sql)
- Dump db
  Using dump.py tool in GOB-Export
  - collection
