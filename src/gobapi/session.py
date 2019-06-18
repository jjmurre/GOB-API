from flask import g  # Safe global storage for sessions

_db_session = None   # Private scoped session


def set_session(db_session):
    """
    Register the scoped session

    :param db_session: scoped_session object
    :return: None
    """
    global _db_session
    _db_session = db_session


def get_session():
    """
    Get a session and store it in the flask global storage

    :return: session instance
    """
    g.session = _db_session()
    return g.session


class ManagedSession:

    def __init__(self):
        pass

    def __enter__(self):
        self._session = _db_session()
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()


def shutdown_session(exception=None):
    """
    Cleanup session factory at the end of a session

    :param exception: any exception
    :return: None
    """
    if exception is not None:
        print(f"Shutdown session, exception: {str(exception)}")
    if hasattr(g, 'session'):
        # REST API
        print("Close REST API session")
        g.session.close()
        delattr(g, 'session')
    else:
        # GraphQL API
        print("Close GraphQL API session")
        _db_session.remove()
