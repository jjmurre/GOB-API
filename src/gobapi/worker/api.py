from flask import send_file, jsonify

from gobapi.worker.response import WorkerResponse


def worker_result(worker_id):
    filename = WorkerResponse.getResponseFile(worker_id)
    if filename:
        return send_file(filename)
    elif WorkerResponse.isWorking(worker_id):
        return f"Worker {worker_id} not finished", 204  # No Content
    else:
        return _worker_not_found(worker_id)


def worker_end(worker_id):
    WorkerResponse.kill(worker_id)
    return "", 204  # No Content


def worker_status(worker_id):
    status = WorkerResponse.getStatus(worker_id)
    if status:
        return jsonify(status)
    else:
        return _worker_not_found(worker_id)


def _worker_not_found(worker_id):
    return f"Worker {worker_id} not found", 404  # Not found
