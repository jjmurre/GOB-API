from functools import reduce
import json
import logging
import uuid

from flask import request

from flask_audit_log.util import get_client_ip

from gobcore.logging.audit_logger import AuditLogger
from gobcore.secure.config import AUTH_PATTERN, REQUEST_ROLES, REQUEST_USER

logger = logging.getLogger()


def get_log_handler():
    return DatabaseHandler()


class DatabaseHandler(logging.StreamHandler):

    def emit(self, record):
        """
        Format the data received from the audit log middleware to match the current temporay storage
        in the database. The source and destination of the message are extracted and the msg is split
        in separate request and response logs.

        Once the audit logs can be stored in Elastic, the handler can be changed.

        The middleware logs message in the following format:
        {
            'audit': {
                'http_request': ....,
                'http_response': ....,
                'user': ....,
                ...
            }
        }
        """
        audit_logger = AuditLogger.get_instance()

        request_uuid = str(uuid.uuid4())

        try:
            msg = json.loads(self.format(record))
        except (json.JSONDecodeError, TypeError):
            # Error transforming msg to json. Logging as str
            source = 'Could not get source from msg'
            destination = 'Could not get destination from msg'
            request_data = response_data = None
        else:
            # Get the source and destination from the middleware log message
            source = get_nested_item(msg, 'audit', 'http_request', 'url')
            destination = get_nested_item(msg, 'audit', 'user', 'ip')
            # Strip the response data from the msg to create request only data and vice versa
            request_data = {k: v for k, v in msg.get('audit', {}).items() if k != 'http_response'}
            response_data = {k: v for k, v in msg.get('audit', {}).items() if k != 'http_response'}

        audit_logger.log_request(
            source=source,
            destination=destination,
            extra_data=request_data,
            request_uuid=request_uuid)

        audit_logger.log_response(
            source=source,
            destination=destination,
            extra_data=response_data,
            request_uuid=request_uuid)


def get_user_from_request() -> dict:
    """
    Gets the user information from the request header set by keycloak
    and returns a dict with the user information for the Datapunt Audit Logger
    """
    user = {
        'authenticated': True if request.headers.get(REQUEST_USER) else False,
        'provider': 'Keycloak',
        'realm': '',
        'email': request.headers.get(f'{AUTH_PATTERN[1:]}Email', ''),
        'roles': request.headers.get(REQUEST_ROLES, []),
        'ip': get_client_ip(request)
    }
    return user


def get_nested_item(data, *keys):
    return reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None, keys, data)
