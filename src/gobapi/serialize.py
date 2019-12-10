import datetime

from gobapi.auth.auth_query import Authority


def datetime_value(dt):
    """Serialize a DateTime

    :param dt: DateTime
    :return: dt as a string in iso format
    """
    # Transform to internal string format and work around issue: https://bugs.python.org/issue13305
    return f"{dt.year:04d}-" + dt.strftime("%m-%dT%H:%M:%S.%f").replace('.000000', '')


def date_value(d):
    """Serialize a Date

    :param d: Date
    :return: d as a string in iso format
    """
    return f"{d.year:04d}-" + d.strftime("%m-%d")


def secure_value(sec_value):
    """Serialize a secure value

    The user authorizations determine the access to the secure value
    :param sec_value: An encrypted secure value
    :return: the unencrypted value if the user has access to to the value, else a no-access string
    """
    authority = Authority(catalog_name=None, collection_name=None)
    value = authority.get_secured_value(sec_value)

    if value is None:
        return None
    elif isinstance(value, datetime.datetime):
        return datetime_value(value)
    else:
        return value
