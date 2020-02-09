from flask import jsonify

from gobapi.storage import exec_statement

from gobapi.dbinfo.statements import DB_STATEMENTS


def get_db_info(info_type):
    statement = DB_STATEMENTS[info_type]
    results = exec_statement(statement)
    return jsonify([dict(result) for result in results])
