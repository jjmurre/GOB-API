from gobcore.secure.config import GOB_ADMIN

GOB_AUTH_SCHEME = {
    # "brk": {
    #     "roles": [GOB_ADMIN]
    # },
    "test_catalogue": {
        "collections": {
            "secure": {
                "attributes": {
                    "secure_number": {
                        "roles": [GOB_ADMIN]
                    },
                    "secure_datetime": {
                        "roles": [GOB_ADMIN]
                    },
                    "secure_reference": {
                        "roles": [GOB_ADMIN]
                    }
                }
            }
        }
    }
}
