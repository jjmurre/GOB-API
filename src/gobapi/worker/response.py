import os
import datetime
import uuid

from pathlib import Path

from flask import request, Response, stream_with_context
from gobcore.message_broker.config import GOB_SHARED_DIR


class WorkerResponse():

    # Write worker files in a folder in GOB_SHARED_DIR
    _WORKER_FILES_DIR = "workerfiles"

    # Recognise worker requests in request header and write worker id in response header
    _WORKER_REQUEST = "X-Worker-Request"
    _WORKER_ID_RESPONSE = "X-Worker-Id"

    # While working, report progress once every seconds
    _YIELD_PROGRESS_INTERVAL = 60

    #
    # Public interface
    #

    def __init__(self):
        self.id = str(uuid.uuid4())
        self._last_progress = None

    def yield_progress(self, filename):
        """
        Yield the size of the given filename every _YIELD_PROGRESS_INTERVAL seconds
        :param filename:
        :return:
        """
        now = datetime.datetime.now()
        if not self._last_progress or (now - self._last_progress).seconds >= self._YIELD_PROGRESS_INTERVAL:
            self._last_progress = now
            yield f"{self._get_file_size(filename)}\n"

    def write_response(self, rows):
        """
        Generator method that writes the given rows to a file

        :param rows:
        :return:
        """
        filename = self._get_filename(self.id)
        tmp_filename = self._get_tmp_filename(self.id)
        sentinel = self._get_sentinel_filename(self.id)

        print(f"INFO: Worker {self.id} started")
        yield f"{self.id}\n"

        success = False
        with open(tmp_filename, "w") as f:
            for row in rows:
                f.write(row)
                if not self._last_progress:
                    print(f"INFO: Worker {self.id} wrote first row")
                yield from self.yield_progress(tmp_filename)
                if os.path.isfile(sentinel):
                    print(f"WARNING: Worker {self.id} aborted")
                    yield f"ABORT\n"
                    break
            else:
                # no break or exception, all rows have successfully been written to file
                success = True

        if success:
            os.rename(tmp_filename, filename)

        if self.is_finished(self.id):
            print(f"INFO: Worker {self.id} OK")
            yield f"{self._get_file_size(filename)}\n"
            yield "OK"
        else:
            self._cleanup(self.id)
            print(f"ERROR: Worker {self.id} FAILURE")
            yield "FAILURE"

    @classmethod
    def stream_with_context(cls, rows, mimetype):
        if request.headers.get(cls._WORKER_REQUEST):
            worker = WorkerResponse()
            response = Response(stream_with_context(worker.write_response(rows)), mimetype='text/plain')
            response.headers[cls._WORKER_ID_RESPONSE] = worker.id
            return response
        else:
            return Response(stream_with_context(rows), mimetype=mimetype)

    @classmethod
    def is_working(cls, worker_id):
        return os.path.isfile(cls._get_tmp_filename(worker_id)) and not cls.is_aborting(worker_id)

    @classmethod
    def is_aborting(cls, worker_id):
        return os.path.isfile(cls._get_sentinel_filename(worker_id))

    @classmethod
    def is_finished(cls, worker_id):
        return os.path.isfile(cls._get_filename(worker_id))

    @classmethod
    def get_status(cls, worker_id):
        if cls.is_finished(worker_id):
            status = "finished"
            size = cls._get_file_size(cls._get_filename(worker_id))
        elif cls.is_working(worker_id):
            status = "working"
            size = cls._get_file_size(cls._get_tmp_filename(worker_id))
        elif cls.is_aborting(worker_id):
            status = "aborting"
            size = None
        else:
            return None

        return {
            "id": worker_id,
            "status": status,
            "size": size
        }

    @classmethod
    def get_response_file(cls, worker_id):
        filename = cls._get_filename(worker_id)
        if os.path.isfile(filename):
            return filename

    @classmethod
    def kill(cls, worker_id):
        if cls.is_finished(worker_id):
            cls._cleanup(worker_id)
        elif cls.is_working(worker_id):
            Path(cls._get_sentinel_filename(worker_id)).touch()

    #
    # Private interface
    #

    @classmethod
    def _get_filename(cls, worker_id):
        return cls._get_base_filename(worker_id)

    @classmethod
    def _get_tmp_filename(cls, worker_id):
        return cls._get_base_filename(worker_id) + ".tmp"

    @classmethod
    def _get_sentinel_filename(cls, worker_id):
        return cls._get_base_filename(worker_id) + ".stop"

    @classmethod
    def _get_base_filename(cls, worker_id):
        dir = os.path.join(GOB_SHARED_DIR, cls._WORKER_FILES_DIR)
        # Create the path if the path not yet exists
        path = Path(dir)
        path.mkdir(exist_ok=True)
        return os.path.join(dir, worker_id)

    @classmethod
    def _get_file_size(cls, filename):
        try:
            return os.path.getsize(filename)
        except FileNotFoundError:
            return 0

    @classmethod
    def _remove_file(cls, filename):
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass

    @classmethod
    def _cleanup(cls, worker_id):
        cls._remove_file(cls._get_sentinel_filename(worker_id))
        cls._remove_file(cls._get_tmp_filename(worker_id))
        cls._remove_file(cls._get_filename(worker_id))
