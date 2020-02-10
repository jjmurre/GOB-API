from flask import jsonify

from gobapi.storage import exec_statement

from gobapi.dbinfo.statements import DB_STATEMENTS


def get_db_info(info_type):
    statement = DB_STATEMENTS.get(info_type)
    if statement:
        results = exec_statement(statement)
        return jsonify([dict(result) for result in results])
    else:
        return "", 404  # Not found
