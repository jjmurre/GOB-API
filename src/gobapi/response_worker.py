import os
import time
import uuid

from threading import Thread
from pathlib import Path

from gobcore.message_broker.config import GOB_SHARED_DIR

class ResponseWorker():

    def __init__(self):
        self.id = str(uuid.uuid1())

    @classmethod
    def kill(cls, id):
        Path(cls.getSentinelFilename(id)).touch()

    @classmethod
    def getTmpFilename(cls, id):
        return cls._getBaseFilename(id) + ".tmp"

    @classmethod
    def getFilename(cls, id):
        return cls._getBaseFilename(id)

    @classmethod
    def getSentinelFilename(cls, id):
        return cls._getBaseFilename(id) + ".stop"

    @classmethod
    def _getBaseFilename(cls, id):
        dir = os.path.join(GOB_SHARED_DIR, "workerfiles")
        # Create the path if the path not yet exists
        path = Path(dir)
        path.mkdir(exist_ok=True)
        return os.path.join(dir, id)

    @classmethod
    def getRequest(cls, id):
        filename = cls.getFilename(id)
        if os.path.isfile(filename):
            return filename

    @classmethod
    def getRequestStatus(cls, id):
        filename = cls.getFilename(id)
        tmp_filename = cls.getTmpFilename(id)

        if os.path.isfile(filename):
            status = "finished"
            size = cls.getFileSize(filename)
        elif os.path.isfile(tmp_filename):
            status = "finished"
            size = cls.getFileSize(tmp_filename)
        else:
            return None

        return {
            "id": id,
            "status": status,
            "size": size
        }

    @classmethod
    def getFileSize(cls, filename):
        try:
            return os.path.getsize(filename)
        except FileNotFoundError:
            return 0

    def handleResponse(self, rows):

        filename = self.getFilename(self.id)
        tmp_filename = self.getTmpFilename(self.id)
        sentinel = self.getSentinelFilename(self.id)

        def writeResponse():
            f = open(tmp_filename, "w")
            for row in rows:
                f.write(row)
                if os.path.isfile(sentinel):
                    break
                time.sleep(2)
            f.close()

        yield self.id
        task = Thread(target=writeResponse)
        task.start()
        while task.is_alive():
            yield f"{self.getFileSize(tmp_filename)}\n"
            time.sleep(5)
        task.join()
        if os.path.isfile(sentinel):
            os.remove(sentinel)
            os.remove(tmp_filename)
            yield "FAILURE"
        else:
            os.rename(tmp_filename, filename)
            yield f"{self.getFileSize(filename)}\n"
            yield "OK"
