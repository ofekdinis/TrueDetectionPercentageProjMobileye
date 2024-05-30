import os
from query_athena import QueryAthena
from sql_queries_generator import SqlQueriesGenerator
import logging
import time
import json
import pandas as pd

class VehicleDetetionPrecentage:
    '''
    This class is used by the outside user or developer
    to create sql query and run it against data found in s3 bucket, using aws athena,
    and format the result and return them as dataframe and save to different files.
    uses the main function generate_true_detection_data,
    that returns the dataframe containing the data for more data analysis.
    '''
    def __init__(self,block_size=10):
        '''
        :param block_size:represnet the seperation of the distance value to ranges,by default for block_size=10,range from 1-10,11-20..91-100, block size is no longer the defined upperbound 100 and lower than 1 lowerbound
        '''
        self.block_size=block_size
        logging.basicConfig(
            filename='vehicle_detection.log',  # Name of the log file
            filemode='a',  # Append mode, use 'w' to overwrite each time
            format='%(asctime)s - %(levelname)s - %(message)s',  # Format of log entries
            level=logging.INFO  # Minimum log level to capture
        )
        logging.info("start logging")
        self.database = "mobileye_detect_vehicle_db"  # Athena database name
        self.bucket_name = "vehicledetectionbucket"
        self.output_location = "s3://vehicledetectionbucket/athena_result/"  # S3 output location
    @staticmethod
    def _format_output_data(json_data):
        '''
        :param json_data: data from this call athena.get_query_result(query_execution_id)
        :return:return dataframe with the formatted data,also create data.json file(not formatted)
        ,data.csv file(formatted and readable) and vehicle_type_bar_chart.png chart
        '''
        # handle results
        logging.info("start format_output_data")
        #save as json
        with open('data.json', 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        #format to csv file
        rows = json_data["ResultSet"]["Rows"]  # get the rows from json
        header = [col["VarCharValue"] for col in rows[0]["Data"]]  # get first row that is the columns names
        data = [[col.get("VarCharValue", "") for col in row["Data"]] for row in rows[1:]]  # get the other data rows

        # Convert the dictionary to a DataFrame
        df = pd.DataFrame(data, columns=header)

        # Removing the "ignore" row
        df = df[df['vehicle_type'] != 'ignore']

        # Display the DataFrame nicely
        df.to_csv('data.csv', index=False)

        #create bar chart file
        VehicleDetetionPrecentage._format_df_for_bar_chart(df)
        logging.info("done format_output_data")
        return df
    @staticmethod
    def _format_df_for_bar_chart(df):
        if str(os.getenv("CREATE_BAR_CHART")).lower()!="true":
            logging.info("Not Creating Bar Char,set CREATE_BAR_CHART=\"true\" to get bar char file")
            return None
        logging.info("Creating Bar Chart")
        import matplotlib.pyplot as plt
        # Convert columns to numeric where possible
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Plotting the DataFrame
        df.set_index('vehicle_type').T.plot(kind='bar', figsize=(10, 7))

        # Setting the title and labels
        plt.title('Vehicle True Detection Percentage Bar Chart')
        plt.xlabel('Distance Range')
        plt.ylabel('True Detection Percentage')
        plt.legend(title='Vehicle Type', bbox_to_anchor=(1.05, 1), loc='upper left')#show all vehicle types

        # Saving the plot to a file
        plt.tight_layout()
        plt.savefig('vehicle_type_bar_chart.png')
    def generate_true_detection_data(self):
        '''
        create sql query and run it,
        for showing detection percentage of different vehicles from data in s3 bucket,
        separate the data by "block_size"
        that represent different ranges of distance or detection the vehicle
        return the result in dataframe and output to files in different formats
        :return:return dataframe with the formatted data,
        also create data.json file(not formatted) ,
        data.csv file(formatted and readable)
        and vehicle_type_bar_chart.png image
        '''
        logging.info("start generate_true_detection_data")
        try:
            athena = QueryAthena()
            sql_query_generator = SqlQueriesGenerator()
            sql_query = sql_query_generator.create_query_by_range(block_size=self.block_size)
            if sql_query==None:
                logging.error("Error: create_query_by_range failed,"
                              "\n please change block_size value and re-run."
                              "\nexisting program")
                exit(1)
            query_execution_id = athena.run_sql_query(sql_query, self.database, self.output_location)  # test
            logging.info("Sleep 30 second, waiting for the query execution to finish")
            time.sleep(30)#Sleep 30 second, waiting for the query execution to finish
            logging.info("done Sleep")
            # query_execution_id="923e6aed-815b-4f31-847d-fdce5bcf02d5"
            json_data = athena.get_query_result(query_execution_id)
            df=VehicleDetetionPrecentage._format_output_data(json_data)
            logging.info("done generate_true_detection_data")
            return df
        except Exception as e:
            logging.error("Error:generate_true_detection_data"
                          " function failed with error:\n"+str(e))
            return None
if __name__ == '__main__':
    vdp=VehicleDetetionPrecentage(block_size=30)
    df=vdp.generate_true_detection_data()
    print("done generate_true_detection_data")


