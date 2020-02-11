from gobcore.secure.config import GOB_ADMIN
from gobcore.secure.config import BRK_DATA_BEPERKT, BRK_DATA_TOTAAL

GOB_AUTH_SCHEME = {
    "brk": {
        "collections": {
            "kadastraleobjecten": {
                "attributes": {
                    "koopsom": {
                        "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
                    },
                    "koopsom_valutacode": {
                        "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
                    },
                    "koopjaar": {
                        "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
                    },
                    "soort_cultuur_bebouwd": {
                        "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
                    },
                    "soort_cultuur_onbebouwd": {
                        "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
                    },
                }
            },
            "kadastralesubjecten": {
                "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT],
                "attributes": {
                    "heeft_bsn_voor": {
                        "roles": [BRK_DATA_TOTAAL]
                    },
                }
            },
            "zakelijkerechten": {
                "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
            },
            "tenaamstellingen": {
                "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
            },
            "aantekeningenrechten": {
                "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
            },
            "aantekeningenkadastraleobjecten": {
                "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
            },
            "stukdelen": {
                "roles": [BRK_DATA_TOTAAL, BRK_DATA_BEPERKT]
            }
        }
    },
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
            },
            "anydata": {
                "attributes": {
                    "auto": {
                        "roles": [GOB_ADMIN]
                    },
                    "functie": {
                        "roles": [GOB_ADMIN]
                    }
                }
            }
        }
    }
}
