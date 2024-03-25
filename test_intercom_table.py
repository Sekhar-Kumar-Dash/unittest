import unittest
from unittest.mock import patch, Mock
import pandas as pd
from mindsdb_sql.parser import ast
from typing import List



class TestArticles(unittest.TestCase):
    def setUp(self):
        self.mock_handler = Mock()
        self.articles = Articles(self.mock_handler)

    @patch('your_module.Articles.get_columns')
    def test_select_with_id(self, mock_get_columns):
        mock_get_columns.return_value = ['id', 'title', 'description']
        mock_response = [{'id': 1, 'title': 'Article 1', 'description': 'Description 1'}]
        self.mock_handler.call_intercom_api.return_value = mock_response

        query = ast.Select(
            targets=[ast.Identifier(('title',))],
            from_table=ast.Identifier(('articles',)),
            where=ast.Expression(
                op='=',
                left=ast.Identifier(('id',)),
                right=ast.Constant(1)
            )
        )
        result = self.articles.select(query)

        expected_result = pd.DataFrame({'title': ['Article 1']})
        pd.testing.assert_frame_equal(result, expected_result)

    @patch('your_module.Articles.get_columns')
    def test_select_without_id(self, mock_get_columns):
        mock_get_columns.return_value = ['id', 'title', 'description']
        mock_response = [
            {'id': 1, 'title': 'Article 1', 'description': 'Description 1'},
            {'id': 2, 'title': 'Article 2', 'description': 'Description 2'},
            {'id': 3, 'title': 'Article 3', 'description': 'Description 3'}
        ]
        self.mock_handler.call_intercom_api.side_effect = [
            {'data': [mock_response[:2]]},
            {'data': [mock_response[2:]]}
        ]

        query = ast.Select(
            targets=[ast.Star()],
            from_table=ast.Identifier(('articles',)),
            limit=ast.Constant(2)
        )
        result = self.articles.select(query)

        expected_result = pd.DataFrame({
            'id': [1, 2],
            'title': ['Article 1', 'Article 2'],
            'description': ['Description 1', 'Description 2']
        })
        pd.testing.assert_frame_equal(result, expected_result)

    def test_insert(self):
        mock_data = {'title': 'New Article', 'description': 'New Description'}
        query = ast.Insert(
            into=ast.Identifier(('articles',)),
            columns=[ast.Identifier(('title',)), ast.Identifier(('description',))],
            values=[
                [ast.Constant('New Article'), ast.Constant('New Description')]
            ]
        )

        self.articles.insert(query)
        self.mock_handler.call_intercom_api.assert_called_once_with(
            endpoint='/articles',
            method='POST',
            data=mock_data
        )

    def test_update(self):
        mock_data = {'title': 'Updated Article'}
        query = ast.Update(
            table=ast.Identifier(('articles',)),
            update_columns={'title': ast.Constant('Updated Article')},
            where=ast.Expression(
                op='=',
                left=ast.Identifier(('id',)),
                right=ast.Constant(1)
            )
        )

        self.articles.update(query)
        self.mock_handler.call_intercom_api.assert_called_once_with(
            endpoint='/articles/1',
            method='PUT',
            data=mock_data
        )

    def test_get_columns(self):
        expected_columns = [
            "type",
            "id",
            "workspace_id",
            "title",
            "description",
            "body",
            "author_id",
            "state",
            "created_at",
            "updated_at",
            "url",
            "parent_id",
            "parent_ids",
            "parent_type",
            "statistics"
        ]
        columns = self.articles.get_columns()
        self.assertListEqual(columns, expected_columns)

        ignore_list = ["type", "id"]
        expected_columns_without_ignore = [
            column for column in expected_columns if column not in ignore_list
        ]
        columns_without_ignore = self.articles.get_columns(ignore=ignore_list)
        self.assertListEqual(columns_without_ignore, expected_columns_without_ignore)
    
    def test_select_with_unknown_target(self):
            query = ast.Select(
                targets=[ast.Constant(1)],  # Invalid target
                from_table=ast.Identifier(('articles',))
            )

            with self.assertRaises(ValueError):
                self.articles.select(query)

    def test_select_with_invalid_where_condition(self):
        query = ast.Select(
            targets=[ast.Identifier(('title',))],
            from_table=ast.Identifier(('articles',)),
            where=ast.Expression(
                op='<>',  # Invalid operator
                left=ast.Identifier(('id',)),
                right=ast.Constant(1)
            )
        )

        with self.assertRaises(ValueError):
            self.articles.select(query)

    def test_insert_with_missing_columns(self):
        query = ast.Insert(
            into=ast.Identifier(('articles',)),
            columns=[ast.Identifier(('title',))],
            values=[
                [ast.Constant('New Article'), ast.Constant('New Description')]  # Missing column
            ]
        )

        with self.assertRaises(IndexError):
            self.articles.insert(query)

    def test_update_with_invalid_where_condition(self):
        query = ast.Update(
            table=ast.Identifier(('articles',)),
            update_columns={'title': ast.Constant('Updated Article')},
            where=ast.Expression(
                op='<>',  # Invalid operator
                left=ast.Identifier(('id',)),
                right=ast.Constant(1)
            )
        )

        with self.assertRaises(NotImplementedError):
            self.articles.update(query)

    def test_get_columns_with_invalid_ignore_type(self):
        with self.assertRaises(TypeError):
            self.articles.get_columns(ignore=1)  

if __name__ == '__main__':
    unittest.main()
