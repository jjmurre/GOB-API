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
    Get a session.

    The returned session will automatically closed at application context end

    :return: session instance
    """
    return _db_session()


def shutdown_session(exception=None):
    """
    Cleanup session factory at the end of a session

    :param exception: any exception
    :return: None
    """
    if exception is not None:
        print(f"Shutdown session, exception: {str(exception)}")
    _db_session.remove()
