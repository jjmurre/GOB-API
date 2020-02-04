from time import sleep

from unittest import TestCase, mock

from gobapi.worker.response import WorkerResponse

class TestResponse(TestCase):

    def test_constructor(self):
        worker_response = WorkerResponse()
        self.assertIsNotNone(worker_response.id)

    def test_writeResponse(self):
        worker_response = WorkerResponse()
        WorkerResponse._YIELD_INTERVAL = 1
        worker_response._write_response = lambda rows: sleep(3)
        worker_response.is_finished = lambda id: False
        result = [r for r in worker_response.write_response([])]
        self.assertTrue("FAILURE" in result)

        worker_response.is_finished = lambda id: True
        result = [r for r in worker_response.write_response([])]
        self.assertTrue("OK" in result)

    @mock.patch("gobapi.worker.response.request")
    @mock.patch("gobapi.worker.response.Response")
    @mock.patch("gobapi.worker.response.stream_with_context")
    def test_stream_with_context(self, mock_stream_with_context, mock_response, mock_request):
        mock_request.headers = {}
        result = WorkerResponse.stream_with_context([], 'any mimetype')
        self.assertEqual(result, mock_response.return_value)
        mock_response.assert_called_with(mock_stream_with_context.return_value, 'any mimetype')

        mock_request.headers = {
            WorkerResponse._WORKER_REQUEST: True
        }
        result = WorkerResponse.stream_with_context([], 'any mimetype')
        self.assertEqual(result, mock_response.return_value)
        mock_response.assert_called_with(mock_stream_with_context.return_value, mimetype='text/plain')

    @mock.patch("gobapi.worker.response.os.path.isfile")
    def test_isWorking(self, mock_isfile):
        mock_isfile.side_effect = [True, False]
        result = WorkerResponse.is_working('any id')
        self.assertTrue(result)

        # working and is aborting
        mock_isfile.side_effect = [True, True]
        result = WorkerResponse.is_working('any id')
        self.assertFalse(result)

    @mock.patch("gobapi.worker.response.os.path.isfile")
    def test_isAborting(self, mock_isfile):
        mock_isfile.return_value = True
        result = WorkerResponse.is_aborting('any id')
        self.assertTrue(result)

        mock_isfile.return_value = False
        result = WorkerResponse.is_aborting('any id')
        self.assertFalse(result)

    @mock.patch("gobapi.worker.response.os.path.isfile")
    def test_isFinished(self, mock_isfile):
        mock_isfile.return_value = True
        result = WorkerResponse.is_finished('any id')
        self.assertTrue(result)

        mock_isfile.return_value = False
        result = WorkerResponse.is_finished('any id')
        self.assertFalse(result)

    @mock.patch("gobapi.worker.response.os.path.isfile")
    def test_getStatus(self, mock_isfile):
        worker_id = 'any  worker id'

        mock_isfile.return_value = True
        result = WorkerResponse.get_status(worker_id)
        self.assertEqual(result, {'id': worker_id, 'status': 'finished', 'size': mock.ANY})

        mock_isfile.side_effect = [False, True, False]
        result = WorkerResponse.get_status(worker_id)
        self.assertEqual(result, {'id': worker_id, 'status': 'working', 'size': mock.ANY})

        mock_isfile.side_effect = [False, False, True]
        result = WorkerResponse.get_status(worker_id)
        self.assertEqual(result, {'id': worker_id, 'status': 'aborting', 'size': mock.ANY})

        mock_isfile.side_effect = [False, False, False]
        result = WorkerResponse.get_status(worker_id)
        self.assertIsNone(result)

    @mock.patch("gobapi.worker.response.os.path.isfile")
    def test_getResponseFile(self, mock_isfile):
        worker_id = 'any  worker id'

        mock_isfile.return_value = True
        result = WorkerResponse.get_response_file(worker_id)
        self.assertEqual(result, WorkerResponse._get_filename(worker_id))

    @mock.patch("gobapi.worker.response.os.path.isfile")
    @mock.patch("gobapi.worker.response.Path")
    def test_kill(self, mock_Path, mock_isfile):
        worker_id = 'any  worker id'

        mock_isfile.return_value = True
        WorkerResponse.kill(worker_id)
        mock_Path.return_value.touch.assert_not_called()

        mock_isfile.side_effect = [False, True, False]
        WorkerResponse.kill(worker_id)
        mock_Path.return_value.touch.assert_called()

    @mock.patch("builtins.open")
    @mock.patch("gobapi.worker.response.os.path.isfile")
    @mock.patch("gobapi.worker.response.os.rename")
    def test__writeResponse(self, mock_rename, mock_isfile, mock_open):
        mock_isfile.return_value = True
        worker = WorkerResponse()
        worker._write_response(['row'])
        mock_rename.assert_not_called()

        mock_isfile.return_value = False
        worker = WorkerResponse()
        worker._write_response(['row'])
        mock_rename.assert_called()