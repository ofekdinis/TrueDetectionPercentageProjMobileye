import time
import boto3
from dotenv import load_dotenv
import os
import logging

class QueryAthena:
    '''
    class for running athena APIs, for generic use
    using credentials and region given by the .env file
    -- run_sql_query - run sql query
    -- get_query_result - get result of a query execution by a given execution id
    '''
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()
        # params for aws api calls
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region_name = os.getenv('REGION_NAME')
    def run_sql_query(self,sql_query,database,output_location):
        '''
        execute the sql query on the given database and output result in output_location
        :param sql_query: sql command as string
        :param database: db name as string
        :param output_location: bucket path in s3
        :return: query_execution_id
        '''
        logging.info('start run sql query')
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
        logging.info('finish run sql query')
        return query_execution_id
    def get_query_result(self,query_execution_id):
        '''
        return the result data from the query using get_query_results api
        :param query_execution_id: id given by this function run_sql_query(..)
        :return: the result as json string without formatting (as is from the api)
        '''
        logging.info('start get sql query result data')
        athena_client = boto3.client('athena',
                                     aws_access_key_id=self.aws_access_key_id,
                                     aws_secret_access_key=self.aws_secret_access_key,
                                     region_name=self.region_name)
        result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        logging.info('done get sql query result data')
        return result
if __name__ == '__main__':
    #main for testing sample query
    logging.basicConfig(filename='query_athena.log', level=logging.INFO)
    logging.info("start logging")
    athena=QueryAthena()
    sql_query = "SELECT * FROM mobileye_detect_vehicle_db.data"  #SQL query
    database = "mobileye_detect_vehicle_db"  # Athena database name
    bucket_name = "vehicledetectionbucket"
    output_location = "s3://vehicledetectionbucket/athena_result/"  #S3 output location

    query_execution_id=athena.run_sql_query(sql_query,database,output_location)#test
    #query_execution_id="0482a444-fcc5-4551-9e6e-75dae28508e8"
    time.sleep(30)
    result=athena.get_query_result(query_execution_id)
