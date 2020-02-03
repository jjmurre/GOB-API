from unittest import TestCase, mock

from gobapi.worker.api import worker_result, worker_end, worker_status, _worker_not_found


@mock.patch("gobapi.worker.api.sleep", lambda n: None)
class TestAPI(TestCase):

    def setUp(self) -> None:
        pass

    @mock.patch("gobapi.worker.api.WorkerResponse")
    @mock.patch("gobapi.worker.api.send_file")
    def test_result(self, mock_send_file, mock_worker_response):
        mock_worker_response.getResponseFile.return_value = None
        mock_worker_response.isWorking.return_value = False
        result = worker_result('any id')
        self.assertEqual(result, _worker_not_found('any id'))

        mock_worker_response.isWorking.return_value = True
        msg, status = worker_result('any id')
        self.assertEqual(msg, "Worker any id not finished")
        self.assertEqual(status, 204)

        mock_send_file.assert_not_called()
        mock_worker_response.getResponseFile.return_value = "any file"
        result = worker_result('any id')
        self.assertEqual(result, mock_send_file.return_value)

    @mock.patch("gobapi.worker.api.WorkerResponse")
    def test_worker_end(self, mock_worker_response):
        mock_worker_response.getStatus.return_value = None
        result = worker_end('any id')
        self.assertEqual(result, _worker_not_found('any id'))

        mock_worker_response.getStatus.return_value = "any status"
        result = worker_end('any id')
        self.assertEqual(result, ("", 204))

    @mock.patch("gobapi.worker.api.jsonify")
    @mock.patch("gobapi.worker.api.WorkerResponse")
    def test_worker_status(self, mock_worker_response, mock_jsonify):
        mock_worker_response.getStatus.return_value = None
        result = worker_status('any id')
        self.assertEqual(result, _worker_not_found('any id'))

        mock_jsonify.assert_not_called()
        mock_worker_response.getStatus.return_value = "any status"
        result = worker_status('any id')
        mock_jsonify.assert_called()
        self.assertEqual(result, mock_jsonify.return_value)
