from gobapi.auth.config import GOB_ADMIN

GOB_AUTH_SCHEME = {
    "test_catalogue": {
        "collections": {
            "secure": {
                "attributes": {
                    "secure_string": {
                        "roles": [GOB_ADMIN]
                    },
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
