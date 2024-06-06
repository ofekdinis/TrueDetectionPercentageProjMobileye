import logging
import os
import time
from dotenv import load_dotenv
import boto3

# Constants
DEFAULT_SLEEP_TIME = 30
MAX_SLEEP_TIME = 120
MIN_SLEEP_TIME = 0
LOWER_BOUND = 1
UPPER_BOUND = 100
SLEEP_TIME_ENV_VAR = 'SLEEP_TIME'
AWS_ACCESS_KEY_ID_ENV_VAR = 'AWS_ACCESS_KEY_ID'
AWS_SECRET_ACCESS_KEY_ENV_VAR = 'AWS_SECRET_ACCESS_KEY'
REGION_NAME_ENV_VAR = 'REGION_NAME'
LOG_FILENAME = '../vehicle_detection.log'
LOG_FILEMODE = 'a'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_LEVEL = logging.INFO
LOG_QUERY_ATHENA_FILENAME = '../query_athena.log'


class SqlQueriesGenerator:
    '''
    This class and file is using SQL queries templates to create a dynamic SQL query,
    that adds lines to the query by a given parameter block_size, and validate its value.
    '''

    def __init__(self):
        logging.basicConfig(
            filename=LOG_FILENAME,  # Name of the log file
            filemode=LOG_FILEMODE,  # Append mode, use 'w' to overwrite each time
            format=LOG_FORMAT,  # Format of log entries
            level=LOG_LEVEL  # Minimum log level to capture
        )

    def _get_sql_detection_range_template(self):
        return ("ROUND((SUM(CASE WHEN distance BETWEEN ###START### AND ###END### "
                "AND detection = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE "
                "WHEN distance BETWEEN ###START### AND ###END### THEN 1 ELSE 0 END), 0)), "
                "3) AS \"###START###-###END###\"")

    def _validate_detection_range_value(self, block_size: int) -> bool:
        # Test data type
        if not isinstance(block_size, int):
            logging.error(f"block_size: {block_size} should be of type int")
            return False
        # Test range
        if block_size < LOWER_BOUND or block_size > UPPER_BOUND:
            logging.error(f"block_size: {block_size} is out of range, should be between "
                          f"{LOWER_BOUND} and {UPPER_BOUND}")
            return False
        return True

    def _get_sql_detection_range_lines(self, block_size: int) -> str:
        '''
        Helper function to create part of the SQL query,
        this part creates multiple lines according to the given block_size.
        Must use the validation function before calling this (_validate_detection_range_value).
        :param block_size: Positive int in range of LOWER_BOUND to UPPER_BOUND represent distance block size
        :return: The SQL lines represent the separation to different distance ranges
        '''
        output_query = ""
        start = LOWER_BOUND
        while start <= UPPER_BOUND:
            end = start + block_size - 1
            if end > UPPER_BOUND:
                end = UPPER_BOUND
            newline = self._get_sql_detection_range_template().replace("###START###", str(start))
            newline = newline.replace("###END###", str(end))
            start += block_size
            if start > UPPER_BOUND:
                output_query += newline + "\n"
            else:
                output_query += newline + ",\n"
        return output_query

    def _get_template_query(self):
        query = '''
            SELECT 
                vehicle_type,
                ###PLACE_HOLDER###
            FROM 
                mobileye_detect_vehicle_db.data
            WHERE 
                distance BETWEEN 1 AND 100
            GROUP BY 
                vehicle_type
            ORDER BY 
                vehicle_type;
                '''
        return query

    def create_query_by_range(self, block_size: int) -> str:
        '''
        Get block size from 1 to 100,
        create a query that contains all the ranges by block size.
        :param block_size: Block size for the ranges
        :return: The new query
        '''
        if not self._validate_detection_range_value(block_size):
            logging.error("Exit creating SQL query, block size is not correct")
            return None
        lines = self._get_sql_detection_range_lines(block_size)
        query = self._get_template_query().replace("###PLACE_HOLDER###", lines)
        return query

    def _test_create_sql_by_block_sizes(self):
        '''
        See logs or use debugger, break point on line "s11 = 'done'"
        See generated SQL query for each block_size,
        copy the string to Sublime/Notepad for easy read.
        :return: Errors in logs, not returning a value, use for internal debugging
        '''
        s1 = sql_query.create_query_by_range(block_size=11)
        s2 = sql_query.create_query_by_range(block_size=0)  # Should fail
        s3 = sql_query.create_query_by_range(block_size=100)
        s4 = sql_query.create_query_by_range(block_size=1000000)  # Should fail
        s5 = sql_query.create_query_by_range(block_size=53)
        s6 = sql_query.create_query_by_range(block_size=99)
        s7 = sql_query.create_query_by_range(block_size=-89)  # Should fail
        s8 = sql_query.create_query_by_range(block_size="10")  # Should fail
        s9 = sql_query.create_query_by_range(block_size=1)
        s10 = sql_query.create_query_by_range(block_size=2)
        s11 = "done"
if __name__ == '__main__':
    #main to test the functions - see logs for the Expected errors
    sql_query=SqlQueriesGenerator()
    #s=sql_query.create_query_by_range(block_size=10)
    sql_query._test_create_sql_by_block_sizes()
