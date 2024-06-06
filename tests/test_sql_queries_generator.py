import pytest
from src.sql_queries_generator import SqlQueriesGenerator
'''
Tests the validation of block sizes.
Tests the creation of SQL queries 
for both valid and invalid block sizes.
'''
# Constants for testing
VALID_BLOCK_SIZES = [5, 10, 20, 50]
INVALID_BLOCK_SIZES = [-1, 0, 101, 1000, "10", None]

@pytest.mark.parametrize("block_size", VALID_BLOCK_SIZES)
def test_validate_detection_range_value_valid(block_size):
    sql_queries = SqlQueriesGenerator()
    assert sql_queries._validate_detection_range_value(block_size) == True

@pytest.mark.parametrize("block_size", INVALID_BLOCK_SIZES)
def test_validate_detection_range_value_invalid(block_size):
    sql_queries = SqlQueriesGenerator()
    assert sql_queries._validate_detection_range_value(block_size) == False

@pytest.mark.parametrize("block_size", VALID_BLOCK_SIZES)
def test_create_query_by_range_valid(block_size):
    sql_queries = SqlQueriesGenerator()
    query = sql_queries.create_query_by_range(block_size)
    assert query is not None
    assert "###PLACE_HOLDER###" not in query

@pytest.mark.parametrize("block_size", INVALID_BLOCK_SIZES)
def test_create_query_by_range_invalid(block_size):
    sql_queries = SqlQueriesGenerator()
    query = sql_queries.create_query_by_range(block_size)
    assert query is None
