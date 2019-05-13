import datetime

from flask import request

from gobcore.secure.user import User


def datetime_value(dt):
    """Serialize a DateTime

    :param dt: DateTime
    :return: dt as a string in iso format
    """
    # Transform to internal string format and work around issue: https://bugs.python.org/issue13305
    return f"{dt.year:04d}-" + dt.strftime("%m-%dT%H:%M:%S.%f")


def date_value(d):
    """Serialize a Date

    :param d: Date
    :return: d as a string in iso format
    """
    return d.isoformat()


def secure_value(sec_value):
    """Serialize a secure value

    The user authorizations determine the access to the secure value
    :param sec_value: An encrypted secure value
    :return: the unencrypted value if the user has access to to the value, else a no-access string
    """
    user = User(request)
    value = sec_value.get_value(user)

    if value is None:
        return None
    elif isinstance(value, datetime.datetime):
        return datetime_value(value)
    else:
        return value
