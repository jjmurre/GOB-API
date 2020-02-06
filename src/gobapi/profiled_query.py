import time

from sqlalchemy import event
from sqlalchemy.engine import Engine


class ProfiledQuery():

    LONG_DURATION = 60 * 10  # 10 minutes or longer

    def __init__(self, statement):
        self.statement = statement
        self.start_time = None
        self.end_time = None

    def set_start(self):
        self.start_time = time.time()

    def set_end(self):
        self.end_time = time.time()

    @property
    def is_complex(self):
        return self.duration > self.LONG_DURATION

    @property
    def duration(self):
        return self.end_time - self.start_time

    def __str__(self):
        def str_time(secs):
            return time.strftime("%H:%M:%S", time.localtime(secs))

        return f"""ProfiledQuery Info
Started:  {str_time(self.start_time)}
Ended:    {str_time(self.end_time)}
Duration: {round(self.duration * 60)} minutes
Statement:
{self.statement}
"""


def activate():
    """
    Activate profiled queries by registering to the before and after cursor events
    :return:
    """
    @event.listens_for(Engine, "before_cursor_execute")
    def before_cursor_execute(conn, cursor, statement,
                              parameters, context, executemany):
        profiled_query = ProfiledQuery(statement)
        profiled_query.set_start()
        conn.info.setdefault('query_info', []).append(profiled_query)

    @event.listens_for(Engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement,
                             parameters, context, executemany):
        profiled_query = conn.info['query_info'].pop(-1)
        profiled_query.set_end()
        if profiled_query.is_complex:
            print("WARNING: Complex query detected")
            print(profiled_query)
