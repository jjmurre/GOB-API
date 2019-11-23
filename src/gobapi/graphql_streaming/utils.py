import re


def to_snake(camel: str):
    return re.sub('([A-Z])', r'_\1', camel).lower()
