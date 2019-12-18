from gobcore.model.metadata import FIELD


class API_FIELD:
    START_VALIDITY_RELATION = f"{FIELD.START_VALIDITY}_relatie"
    END_VALIDITY_RELATION = f"{FIELD.END_VALIDITY}_relatie"


API_FIELD_DESCRIPTION = {
    FIELD.SOURCE_VALUE: "De bronwaarde die als basis dient voor deze relatie",
    FIELD.SOURCE_INFO: "De extra waarden meegegeven vanuit de bron naast de bronwaarde voor deze relatie",
    API_FIELD.START_VALIDITY_RELATION: "De datum waarop deze relatie is ontstaan",
    API_FIELD.END_VALIDITY_RELATION: "De datum waarop deze relatie is geëindigd",
}
