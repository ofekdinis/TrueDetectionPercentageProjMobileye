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


class QueryAthena:
    '''
    Class for running Athena APIs, for generic use
    using credentials and region given by the .env file.
    -- run_sql_query - run SQL query
    -- get_query_result - get result of a query execution by a given execution id
    '''
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()
        # Params for AWS API calls
        self.aws_access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_VAR)
        self.aws_secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_VAR)
        self.region_name = os.getenv(REGION_NAME_ENV_VAR)
        self.sleep_time = self._get_sleep_time()

    def _get_sleep_time(self):
        sleep_time = os.getenv(SLEEP_TIME_ENV_VAR, str(DEFAULT_SLEEP_TIME))
        try:
            sleep_time = int(sleep_time)
        except ValueError:
            logging.warning(f"Invalid {SLEEP_TIME_ENV_VAR} value: {sleep_time}, defaulting to {DEFAULT_SLEEP_TIME} seconds.")
            sleep_time = DEFAULT_SLEEP_TIME

        if sleep_time < MIN_SLEEP_TIME:
            logging.warning(f"{SLEEP_TIME_ENV_VAR} value is less than {MIN_SLEEP_TIME}: {sleep_time}, defaulting to {DEFAULT_SLEEP_TIME} seconds.")
            sleep_time = DEFAULT_SLEEP_TIME
        elif sleep_time > MAX_SLEEP_TIME:
            logging.warning(f"{SLEEP_TIME_ENV_VAR} value exceeds {MAX_SLEEP_TIME} seconds: {sleep_time}, capping to {MAX_SLEEP_TIME} seconds.")
            sleep_time = MAX_SLEEP_TIME

        return sleep_time

    def run_sql_query(self, sql_query, database, output_location):
        '''
        Execute the SQL query on the given database and output result in output_location.
        :param sql_query: SQL command as string
        :param database: DB name as string
        :param output_location: Bucket path in S3
        :return: query_execution_id
        '''
        logging.info('Start run SQL query')
        athena_client = boto3.client('athena',
                                     aws_access_key_id=self.aws_access_key_id,
                                     aws_secret_access_key=self.aws_secret_access_key,
                                     region_name=self.region_name)
        # Start the query execution
        response = athena_client.start_query_execution(
            QueryString=sql_query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': output_location
            }
        )
        query_execution_id = response['QueryExecutionId']
        logging.info(f'Started query with execution ID: {query_execution_id}')
        logging.info('Finish run SQL query')
        return query_execution_id

    def get_query_result(self, query_execution_id):
        '''
        Return the result data from the query using get_query_results API.
        :param query_execution_id: ID given by this function run_sql_query(..)
        :return: The result as JSON string without formatting (as is from the API)
        '''
        logging.info('Start get SQL query result data')
        athena_client = boto3.client('athena',
                                     aws_access_key_id=self.aws_access_key_id,
                                     aws_secret_access_key=self.aws_secret_access_key,
                                     region_name=self.region_name)
        result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        logging.info('Done get SQL query result data')
        return result


if __name__ == '__main__':
    # Main for testing sample query
    logging.basicConfig(filename=LOG_QUERY_ATHENA_FILENAME, level=logging.INFO)
    logging.info("Start logging")
    athena = QueryAthena()
    sql_query = "SELECT * FROM mobileye_detect_vehicle_db.data"  # SQL query
    database = "mobileye_detect_vehicle_db"  # Athena database name
    bucket_name = "vehicledetectionbucket"
    output_location = "s3://vehicledetectionbucket/athena_result/"  # S3 output location

    query_execution_id = athena.run_sql_query(sql_query, database, output_location)  # Test
    logging.info(f"Sleep for {athena.sleep_time} seconds, waiting for the query execution to finish")
    time.sleep(athena.sleep_time)
    result = athena.get_query_result(query_execution_id)