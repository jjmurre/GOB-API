from time import sleep
from flask import send_file, jsonify

from gobapi.worker.response import WorkerResponse


def worker_result(worker_id):
    """
    Get the result of a worker.

    If the worker has finished the worker file is returned.

    :param worker_id:
    :return:
    """
    filename = WorkerResponse.getResponseFile(worker_id)
    if filename:
        return send_file(filename)
    elif WorkerResponse.isWorking(worker_id):
        return f"Worker {worker_id} not finished", 204  # No Content
    else:
        return _worker_not_found(worker_id)


def worker_end(worker_id):
    """
    End a running worker

    :param worker_id:
    :return:
    """
    status = WorkerResponse.getStatus(worker_id)
    if status:
        WorkerResponse.kill(worker_id)
        return "", 204  # No Content
    else:
        return _worker_not_found(worker_id)


def worker_status(worker_id):
    """
    Returns the status of a worker: dict(id, status, size)

    :param worker_id:
    :return:
    """
    status = WorkerResponse.getStatus(worker_id)
    if status:
        return jsonify(status)
    else:
        return _worker_not_found(worker_id)


def _worker_not_found(worker_id):
    """
    Returns a generic worker not found Response.

    :param worker_id:
    :return:
    """
    sleep(1)  # penalty time to protect against brute force attacks
    return f"Worker {worker_id} not found", 404  # Not found
