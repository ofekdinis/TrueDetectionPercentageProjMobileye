import pytest
from src.query_athena import QueryAthena
from unittest.mock import patch, MagicMock
'''
Tests the _get_sleep_time method for both valid and invalid inputs.
Uses unittest.mock to mock boto3.client and test run_sql_query 
and get_query_result methods without making actual AWS calls.
'''
# Constants for testing
VALID_SLEEP_TIME = [0, 30, 60, 90, 120]
INVALID_SLEEP_TIME = [-1, 121, "invalid", None]

@pytest.mark.parametrize("sleep_time", VALID_SLEEP_TIME)
def test_get_sleep_time_valid(sleep_time):
    with patch.dict('os.environ', {'SLEEP_TIME': str(sleep_time)}):
        query_athena = QueryAthena()
        assert query_athena._get_sleep_time() == min(max(sleep_time, 0), 120)

@pytest.mark.parametrize("sleep_time", INVALID_SLEEP_TIME)
def test_get_sleep_time_invalid(sleep_time):
    with patch.dict('os.environ', {'SLEEP_TIME': str(sleep_time)}):
        query_athena = QueryAthena()
        assert query_athena._get_sleep_time() == 30

def test_run_sql_query():
    with patch('boto3.client') as mock_boto_client:
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        mock_client.start_query_execution.return_value = {'QueryExecutionId': 'test_id'}

        query_athena = QueryAthena()
        query_execution_id = query_athena.run_sql_query('SELECT * FROM test_db', 'test_db', 's3://test-bucket')

        assert query_execution_id == 'test_id'
        mock_client.start_query_execution.assert_called_once()

def test_get_query_result():
    with patch('boto3.client') as mock_boto_client:
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        mock_client.get_query_results.return_value = {'ResultSet': 'test_result'}

        query_athena = QueryAthena()
        result = query_athena.get_query_result('test_id')

        assert result == {'ResultSet': 'test_result'}
        mock_client.get_query_results.assert_called_once_with(QueryExecutionId='test_id')
