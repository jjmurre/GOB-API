import os
import time
import uuid

from threading import Thread, Event
from pathlib import Path

from flask import request, Response, stream_with_context
from gobcore.message_broker.config import GOB_SHARED_DIR


class WorkerResponse():

    # Write worker files in a folder in GOB_SHARED_DIR
    _WORKER_FILES_DIR = "workerfiles"

    # Recognise worker requests in request header and write worker id in response header
    _WORKER_REQUEST = "X-Worker-Request"
    _WORKER_ID_RESPONSE = "X-Worker-Id"

    # While the thread is working, report progress and check thread status once every seconds
    _YIELD_INTERVAL = 60
    _CHECK_THREAD = 1

    #
    # Public interface
    #

    def __init__(self):
        self.id = str(uuid.uuid4())

    def writeResponse(self, rows):
        filename = self._getFilename(self.id)
        tmp_filename = self._getTmpFilename(self.id)
        sentinel = self._getSentinelFilename(self.id)

        yield f"{self.id}\n"

        task = Thread(target=lambda: self._writeResponse(rows))
        task.start()

        yield_interval = 0
        while task.is_alive():
            if yield_interval >= self._YIELD_INTERVAL:
                yield f"{self._getFileSize(tmp_filename)}\n"
                yield_interval = 0
            else:
                time.sleep(self._CHECK_THREAD)
                yield_interval += self._CHECK_THREAD

        task.join()

        if not self.isFinished(self.id):
            self._removeFile(sentinel)
            self._removeFile(tmp_filename)
            self._removeFile(filename)
            yield "FAILURE"
        elif self.isFinished(self.id):
            yield f"{self._getFileSize(filename)}\n"
            yield "OK"

    @classmethod
    def response(cls, rows, mimetype):
        if request.headers.get(cls._WORKER_REQUEST):
            worker = WorkerResponse()
            response = Response(stream_with_context(worker.writeResponse(rows)), mimetype='text/plain')
            response.headers['X-Worker-Id'] = worker.id
            return response
        else:
            return Response(rows, mimetype)

    @classmethod
    def isWorking(cls, id):
        return os.path.isfile(cls._getTmpFilename(id)) and not cls.isAborting(id)

    @classmethod
    def isAborting(cls, id):
        return os.path.isfile(cls._getSentinelFilename(id))

    @classmethod
    def isFinished(cls, id):
        return os.path.isfile(cls._getFilename(id))

    @classmethod
    def getStatus(cls, id):
        if cls.isFinished(id):
            status = "finished"
            size = cls._getFileSize(cls._getFilename(id))
        elif cls.isWorking(id):
            status = "working"
            size = cls._getFileSize(cls._getTmpFilename(id))
        elif cls.isAborting(id):
            status = "aborting"
            size = None
        else:
            return None

        return {
            "id": id,
            "status": status,
            "size": size
        }

    @classmethod
    def getResponseFile(cls, id):
        filename = cls._getFilename(id)
        if os.path.isfile(filename):
            return filename

    @classmethod
    def kill(cls, id):
        if cls.isFinished(id):
            os.remove(cls._getFilename(id))
        elif cls.isWorking(id):
            Path(cls._getSentinelFilename(id)).touch()

    #
    # Private interface
    #

    def _writeResponse(self, rows):
        filename = self._getFilename(self.id)
        tmp_filename = self._getTmpFilename(self.id)
        sentinel = self._getSentinelFilename(self.id)

        success = False
        with open(tmp_filename, "w") as f:
            for row in rows:
                f.write(row)
                if os.path.isfile(sentinel):
                    break
                # time.sleep(10)
            else:
                # no break or exception, all rows have successfully been written to file
                success = True

        if success:
            os.rename(tmp_filename, filename)

    @classmethod
    def _getFilename(cls, id):
        return cls._getBaseFilename(id)

    @classmethod
    def _getTmpFilename(cls, id):
        return cls._getBaseFilename(id) + ".tmp"

    @classmethod
    def _getSentinelFilename(cls, id):
        return cls._getBaseFilename(id) + ".stop"

    @classmethod
    def _getBaseFilename(cls, id):
        dir = os.path.join(GOB_SHARED_DIR, cls._WORKER_FILES_DIR)
        # Create the path if the path not yet exists
        path = Path(dir)
        path.mkdir(exist_ok=True)
        return os.path.join(dir, id)

    @classmethod
    def _getFileSize(cls, filename):
        try:
            return os.path.getsize(filename)
        except FileNotFoundError:
            return 0

    @classmethod
    def _removeFile(cls, filename):
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
