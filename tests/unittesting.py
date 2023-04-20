import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from app.utils import *

class TestGetMargins(unittest.TestCase):
    
    def setUp(self):
        self.report_name = "sample_report"
        self.table = "sample_table"
        self.margin = "sample_margin"
        self.date = "2021-09-01"
        self.time_of_day = "morning"
        self.database = DATABASE

    @patch("app.utils.create_alchemy_connection")
    @patch("app.utils.query_generator")
    @patch("pandas.read_sql_query")
    def test_get_margins(self, mock_read_sql_query, mock_query_generator, mock_create_alchemy_connection):
        # Mock the behavior of external dependencies
        mock_connection = MagicMock()
        mock_create_alchemy_connection.return_value = mock_connection
        mock_query = "SELECT * FROM sample_table WHERE margin='sample_margin' AND date='2021-09-01';"
        mock_query_generator.return_value = mock_query
        mock_df = pd.DataFrame({"margin": ["sample_margin"], "date": ["2021-09-01"]})
        mock_read_sql_query.return_value = mock_df

        # Call the function
        result = get_margins(self.report_name, self.table, self.margin, self.date, self.time_of_day, self.database)

        # Check if the function works as expected
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.name, f"{self.report_name}_{self.margin}")
        self.assertEqual(len(result), 1)

        # Check if the external dependencies were called with the correct arguments
        mock_create_alchemy_connection.assert_called_once_with(self.database)
        mock_query_generator.assert_called_once_with(self.table, self.margin, self.date, self.time_of_day)
        mock_read_sql_query.assert_called_once_with(mock_query, mock_connection)

    @patch("app.utils.create_alchemy_connection")
    def test_get_margins_no_connection(self, mock_create_alchemy_connection):
        # Mock the behavior of create_alchemy_connection to return None
        mock_create_alchemy_connection.return_value = None

        # Call the function
        result = get_margins(self.report_name, self.table, self.margin, self.date, self.time_of_day, self.database)

        # Check if the function returns None when no connection is established
        self.assertIsNone(result)

if __name__ == "__main__":
    unittest.main()