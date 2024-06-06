import time
import boto3
from dotenv import load_dotenv
import os
import logging


class QueryAthena:
    '''
    Class for running Athena APIs, for generic use
    using credentials and region given by the .env file.
    -- run_sql_query - Run SQL query
    -- get_query_result - Get result of a query execution by a given execution ID
    '''

    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()
        # Params for AWS API calls
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region_name = os.getenv('REGION_NAME')

    def run_sql_query(self, sql_query, database, output_location):
        '''
        Execute the SQL query on the given database and output result in output_location
        :param sql_query: SQL command as string
        :param database: DB name as string
        :param output_location: Bucket path in S3
        :return: query_execution_id
        '''
        logging.info('Start running SQL query')
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
        logging.info('Finished running SQL query')
        return query_execution_id

    def get_query_result(self, query_execution_id):
        '''
        Return the result data from the query using get_query_results API
        :param query_execution_id: ID given by the run_sql_query function
        :return: The result as JSON string without formatting (as is from the API)
        '''
        logging.info('Start getting SQL query result data')
        athena_client = boto3.client('athena',
                                     aws_access_key_id=self.aws_access_key_id,
                                     aws_secret_access_key=self.aws_secret_access_key,
                                     region_name=self.region_name)
        result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        logging.info('Done getting SQL query result data')
        return result


if __name__ == '__main__':
    # Main for testing sample query
    logging.basicConfig(filename='../query_athena.log', level=logging.INFO)
    logging.info("Start logging")
    athena = QueryAthena()
    sql_query = "SELECT * FROM mobileye_detect_vehicle_db.data"  # SQL query
    database = "mobileye_detect_vehicle_db"  # Athena database name
    bucket_name = "vehicledetectionbucket"
    output_location = "s3://vehicledetectionbucket/athena_result/"  # S3 output location

    query_execution_id = athena.run_sql_query(sql_query, database, output_location)  # Test
    # query_execution_id = "0482a444-fcc5-4551-9e6e-75dae28508e8"
    time.sleep(30)
    result = athena.get_query_result(query_execution_id)
