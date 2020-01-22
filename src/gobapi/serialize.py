
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
