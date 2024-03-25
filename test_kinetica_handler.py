import unittest
from unittest.mock import patch, MagicMock
from collections import OrderedDict

from mindsdb.integrations.handlers.postgres_handler import Handler as PostgresHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

class TestKineticaHandler(unittest.TestCase):
    def setUp(self):
        self.name = 'test_name'
        self.kwargs = {'arg1': 'value1', 'arg2': 'value2'}

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler.__init__')
    def test_init(self, mock_postgres_init):
        from mindsdb.integrations.handlers.kinetica import KineticaHandler

        handler = KineticaHandler(self.name, **self.kwargs)

        mock_postgres_init.assert_called_once_with(self.name, **self.kwargs)
        self.assertEqual(handler.name, 'kinetica')

        expected_connection_args = OrderedDict(
            user={
                'type': ARG_TYPE.STR,
                'description': 'The user name used to authenticate with the Kinetica server.',
                'required': True,
                'label': 'User',
            },
            password={
                'type': ARG_TYPE.PWD,
                'description': 'The password to authenticate the user with the Kinetica server.',
                'required': True,
                'label': 'Password',
            },
            database={
                'type': ARG_TYPE.STR,
                'description': 'The database name to use when connecting with the Kinetica server.',
                'required': True,
                'label': 'Database',
            },
            host={
                'type': ARG_TYPE.STR,
                'description': "The host name or IP address of the Kinetica server. NOTE: use '127.0.0.1' instead of 'localhost' to connect to local server.",
                'required': True,
                'label': 'Host',
            },
            port={
                'type': ARG_TYPE.INT,
                'description': 'The TCP/IP port of the Kinetica server. Must be an integer.',
                'required': True,
                'label': 'Port',
            },
            schema={
                'type': ARG_TYPE.STR,
                'description': 'The schema in which objects are searched first.',
                'required': False,
                'label': 'Schema',
            },
            sslmode={
                'type': ARG_TYPE.STR,
                'description': 'sslmode that will be used for connection.',
                'required': False,
                'label': 'sslmode',
            },
        )
        self.assertEqual(handler.connection_args, expected_connection_args)

        expected_connection_args_example = OrderedDict(
            host='127.0.0.1', port=5432, user='root', password='password', database='database'
        )
        self.assertEqual(handler.connection_args_example, expected_connection_args_example)

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler.__init__')
    def test_init_with_mocked_postgres_handler(self, mock_postgres_init):
        mock_postgres_handler = MagicMock(spec=PostgresHandler)
        with patch('mindsdb.integrations.handlers.kinetica.PostgresHandler', return_value=mock_postgres_handler):
            from mindsdb.integrations.handlers.kinetica import KineticaHandler

            handler = KineticaHandler(self.name, **self.kwargs)

            mock_postgres_init.assert_called_once_with(self.name, **self.kwargs)
            mock_postgres_handler.assert_called_once()
            self.assertEqual(handler.name, 'kinetica')

    def test_connection_args_required_fields(self):
        from mindsdb.integrations.handlers.kinetica import KineticaHandler

        handler = KineticaHandler(self.name, **self.kwargs)
        required_fields = ['user', 'password', 'database', 'host', 'port']

        for field in required_fields:
            self.assertIn(field, handler.connection_args)
            self.assertTrue(handler.connection_args[field]['required'])

    def test_connection_args_optional_fields(self):
        from mindsdb.integrations.handlers.kinetica import KineticaHandler

        handler = KineticaHandler(self.name, **self.kwargs)
        optional_fields = ['schema', 'sslmode']

        for field in optional_fields:
            self.assertIn(field, handler.connection_args)
            self.assertFalse(handler.connection_args[field]['required'])

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler.query')
    def test_query(self, mock_query):
        from mindsdb.integrations.handlers.kinetica import KineticaHandler

        handler = KineticaHandler(self.name, **self.kwargs)
        query = "SELECT * FROM table;"
        handler.query(query)

        mock_query.assert_called_once_with(query)
    def test_execute_query(self):
        from mindsdb.integrations.handlers.kinetica import KineticaHandler

        handler = KineticaHandler(self.name, **self.kwargs)
        query = "SELECT * FROM table;"
        expected_result = [(1, 'value1'), (2, 'value2')]

        mock_postgres_handler = MagicMock(spec=PostgresHandler)
        mock_postgres_handler.query.return_value = expected_result
        with patch('mindsdb.integrations.handlers.kinetica.PostgresHandler', return_value=mock_postgres_handler):
            result = handler.execute_query(query)

            self.assertEqual(result, expected_result)
            mock_postgres_handler.query.assert_called_once_with(query)

    def test_create_table(self):
        from mindsdb.integrations.handlers.kinetica import KineticaHandler

        handler = KineticaHandler(self.name, **self.kwargs)
        table_name = 'new_table'
        columns = [('id', 'INTEGER'), ('name', 'VARCHAR(50)')]

        mock_postgres_handler = MagicMock(spec=PostgresHandler)
        with patch('mindsdb.integrations.handlers.kinetica.PostgresHandler', return_value=mock_postgres_handler):
            handler.create_table(table_name, columns)

            create_table_query = "CREATE TABLE {} ({});".format(
                table_name,
                ', '.join([' '.join(col) for col in columns])
            )
            mock_postgres_handler.query.assert_called_once_with(create_table_query)

    def test_drop_table(self):
        from mindsdb.integrations.handlers.kinetica import KineticaHandler

        handler = KineticaHandler(self.name, **self.kwargs)
        table_name = 'table_to_drop'

        mock_postgres_handler = MagicMock(spec=PostgresHandler)
        with patch('mindsdb.integrations.handlers.kinetica.PostgresHandler', return_value=mock_postgres_handler):
            handler.drop_table(table_name)

            drop_table_query = "DROP TABLE {};".format(table_name)
            mock_postgres_handler.query.assert_called_once_with(drop_table_query)    

if __name__ == '__main__':
    unittest.main()
