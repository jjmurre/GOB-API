from flask import request

REQUEST_USER = 'X-Auth-Userid'
REQUEST_ROLE = 'X-Auth-Roles'


def secure_route(rule, func):
    """
    Secure routes start with API_SECURE_BASE_PATH and are protected by gatekeeper

    The headers that are used to identify the user and/or role should be present

    :param func:
    :return:
    """
    def wrapper(*args, **kwargs):
        userid = request.headers.get(REQUEST_USER)
        roles = request.headers.get(REQUEST_ROLE)
        if userid is None:
            # Check if the user is authenticated => 401 Unauthorized
            return "Not logged in", 401
        elif roles is None:
            # Check if the user is authorized => 403 Forbidden
            return "Insufficient privileges", 403
        else:
            return func(*args, **kwargs)
    wrapper.__name__ = f"secure_{func.__name__}"
    return wrapper


def _report_fraud(rule, *args, **kwargs):
    """
    Report fraud

    Print an error and request information

    :param rule:
    :param args:
    :param kwargs:
    :return:
    """
    print(f"ERROR: FRAUD DETECTED FOR RULE: {rule}", args, kwargs)
    dump_attrs = ['method', 'remote_addr', 'remote_user', 'headers']
    for attr in dump_attrs:
        print(attr, getattr(request, attr))


def public_route(rule, func, *args, **kwargs):
    """
    Public routes start with API_BASE_PATH and are not protected by gatekeeper

    The headers that are used to identify the user and/or role should NOT be present.
    If any of these headers are present that means that these headers are falsified
    The ip-address and any other identifying information should be reported

    :param func:
    :param args:
    :param kwargs:
    :return:
    """
    def wrapper(*args, **kwargs):
        userid = request.headers.get(REQUEST_USER)
        roles = request.headers.get(REQUEST_ROLE)
        if userid or roles:
            _report_fraud(rule, *args, **kwargs)
            return "Compromised headers detected!", 403
        else:
            return func(*args, **kwargs)
    wrapper.__name__ = f"public_{func.__name__}"
    return wrapper
