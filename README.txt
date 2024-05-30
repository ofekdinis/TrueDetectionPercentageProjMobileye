Big Data Analytics team – Technical interview

Program description:

Data inputs:
The program use data found in s3 bucket and schema from glue (already defined)

Computes:
Create sql query and run it in aws athena,
for showing detection percentage of different vehicles from data in s3 bucket,
separate the data by "block_size" (given by the user,* see notes below)
that represent different ranges of distance or detection the vehicle

Outputs:
return the data results in dataframe object and
output to files in different formats:
    1. dataframe with the formatted data, (returned value from function)
    2. data.json file (data as is from athena) ,
    3. data.csv file (formatted and readable)
    4. vehicle_type_bar_chart.png image , (optional, by default is create,with "true" value in the .env settings)


*Notes:
"block_size" is a range distance,that can be given by the user,
the total range is between 1 and 100,
the default separation is by block_size=10,
separate the output to 1-10,11-20..91-100.

Initial errors and validations:
the input is validated, follow the range given 1-100,
errors will be shown in the "vehicle_detection.log" file, if the
"block_size" value is not correct or general error happened at runtime.





How to run:
installing the requirements.txt file is needed inorder to run the project,
by running the commands in terminal:
pip install --upgrade pip
pip install -r requirements.txt



here is an example for a main function with block_size=10:

from vehicle_detection_by_size import VehicleDetetionPrecentage
if __name__ == '__main__':
    vdp=VehicleDetetionPrecentage(block_size=10)
    df=vdp.generate_true_detection_data()
    print("done generate_true_detection_data")


.env and credentials:
please past those value to the .env file and save it in safe place:
-given in email or in env file, not in git.


How to run tests for developers:
- see the run information in the "vehicle_detection.log" file.
- additional file for testing the sql_query block_size value in test_sql_queries.py ,
simply run pytest test_sql_queries.py in terminal.
- main function are available to test each file separately.
- sql_queries_generator contain another test function, (additional info in the function description)
    can be run or debug as follows:
    sql_query=SqlQueriesGenerator()
    sql_query._test_create_sql_by_block_sizes()
