import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from cohere_handler import CohereHandler

class TestCohereHandler(unittest.TestCase):
    def setUp(self):
        self.handler = CohereHandler()
        self.handler.model_storage = MagicMock()
        self.handler.engine_storage = MagicMock()
        self.args = {
            'using': {
                'column': 'text',
                'task': 'text-summarization'
            },
            'target': 'summary'
        }
        self.handler.model_storage.json_get.return_value = self.args

    @patch('cohere_handler.get_api_key')
    @patch('cohere_handler.cohere.Client')
    def test_predict_text_summary(self, mock_client, mock_get_api_key):
        mock_get_api_key.return_value = 'test_api_key'
        mock_response = MagicMock()
        mock_response.summary = 'Test summary'
        mock_client().summarize.return_value = mock_response

        test_data = pd.DataFrame({'text': ['This is a test text.']})
        result = self.handler.predict(test_data)

        self.assertEqual(result['summary'].iloc[0], 'Test summary')

    @patch('cohere_handler.get_api_key')
    @patch('cohere_handler.cohere.Client')
    def test_predict_text_generation(self, mock_client, mock_get_api_key):
        mock_get_api_key.return_value = 'test_api_key'
        mock_response = MagicMock()
        mock_response.generations = [MagicMock(text='Generated text')]
        mock_client().generate.return_value = mock_response

        self.args['using']['task'] = 'text-generation'
        test_data = pd.DataFrame({'text': ['This is a test text.']})
        result = self.handler.predict(test_data)

        self.assertEqual(result['text-generation'].iloc[0], 'Generated text')

    @patch('cohere_handler.get_api_key')
    @patch('cohere_handler.cohere.Client')
    def test_predict_language(self, mock_client, mock_get_api_key):
        mock_get_api_key.return_value = 'test_api_key'
        mock_response = MagicMock()
        mock_response.results = [MagicMock(language_name='English')]
        mock_client().detect_language.return_value = mock_response

        self.args['using']['task'] = 'language-detection'
        test_data = pd.DataFrame({'text': ['This is a test text.']})
        result = self.handler.predict(test_data)

        self.assertEqual(result['language-detection'].iloc[0], 'English')

    def test_create_raises_exception(self):
        with self.assertRaises(Exception):
            self.handler.create(target='target', args={})

    def test_create_sets_args(self):
        args = {'using': {'column': 'text', 'task': 'text-summarization'}}
        self.handler.create(target='summary', args=args)
        self.handler.model_storage.json_set.assert_called_with('args', args)

    def test_predict_raises_exception_for_missing_column(self):
        test_data = pd.DataFrame({'text_wrong': ['This is a test text.']})
        with self.assertRaises(RuntimeError):
            self.handler.predict(test_data)

    def test_predict_raises_exception_for_unsupported_task(self):
        self.args['using']['task'] = 'unsupported_task'
        test_data = pd.DataFrame({'text': ['This is a test text.']})
        with self.assertRaises(Exception):
            self.handler.predict(test_data)

    @patch('cohere_handler.get_api_key')
    def test_predict_text_summary_with_invalid_api_key(self, mock_get_api_key):
        mock_get_api_key.return_value = None
        test_data = pd.DataFrame({'text': ['This is a test text.']})
        result = self.handler.predict(test_data)
        self.assertTrue(result.empty)

    @patch('cohere_handler.get_api_key')
    @patch('cohere_handler.cohere.Client')
    def test_predict_text_summary_with_empty_text(self, mock_client, mock_get_api_key):
        mock_get_api_key.return_value = 'test_api_key'
        mock_response = MagicMock()
        mock_response.summary = ''
        mock_client().summarize.return_value = mock_response

        test_data = pd.DataFrame({'text': ['']})
        result = self.handler.predict(test_data)

        self.assertEqual(result['summary'].iloc[0], '')

    @patch('cohere_handler.get_api_key')
    def test_get_api_key_strict_mode(self, mock_get_api_key):
        mock_get_api_key.return_value = None
        with self.assertRaises(Exception):
            self.handler.predict_text_summary('Test text')

if __name__ == '__main__':
    unittest.main()
