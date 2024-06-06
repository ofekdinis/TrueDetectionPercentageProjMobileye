import pytest
from src.sql_queries_generator import SqlQueriesGenerator

"""
This file is for unit testing the sql_queries generator functions
focusing on parameter block_size range correctness
"""

@pytest.mark.parametrize("block_size, expected", [
    (SqlQueriesGenerator.UPPER_BOUND + 1, False),
    (5, True),  # Example of a valid block_size, modify based on actual valid value logic
    (100, True), # Example of a valid block_size, modify based on actual valid value logic
    (SqlQueriesGenerator.LOWER_BOUND - 1, False),
    (SqlQueriesGenerator.LOWER_BOUND - 1000, False)
])
def test_validate_detection_range_value(block_size, expected):
    sql_queries = SqlQueriesGenerator()
    assert sql_queries._validate_detection_range_value(block_size) == expected
