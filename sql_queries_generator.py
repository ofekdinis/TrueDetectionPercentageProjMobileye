import logging
class SqlQueriesGenerator:
    '''
    This Class and file is using sql queries templates to create a dynamic sql query,
    that add lines to the query by a given parameter block_size, and validate its value.
    '''
    LOWER_BOUND=1
    UPPER_BOUND=100
    def __init__(self):
        logging.basicConfig(
            filename='vehicle_detection.log',  # Name of the log file
            filemode='a',  # Append mode, use 'w' to overwrite each time
            format='%(asctime)s - %(levelname)s - %(message)s',  # Format of log entries
            level=logging.INFO  # Minimum log level to capture
        )
    def _get_sql_detection_range_template(self):
       return "ROUND((SUM(CASE WHEN distance BETWEEN ###START### AND ###END### AND detection = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN distance BETWEEN ###START### AND ###END### THEN 1 ELSE 0 END), 0)), 3) AS \"###START###-###END###\""
    def _validate_detection_range_value(self,block_size:int) ->bool:
        #test data type
        if isinstance(block_size, int) == False:
            logging.error("block_size:"+str(block_size)+" should be of type int")
            return False
        #test range
        if block_size<SqlQueriesGenerator.LOWER_BOUND or block_size>SqlQueriesGenerator.UPPER_BOUND:#block_size out of range
            logging.error("block_size:"+str(block_size)+" is out of range should be between "+str(SqlQueriesGenerator.LOWER_BOUND)+" and "+str(SqlQueriesGenerator.UPPER_BOUND))
            return False
        return True

    def _get_sql_detection_range_lines(self,block_size:int) ->str:
        '''
        helper function to create part of the sql query,
        this part create multiple lines according to the given block_size
        must use the validation function before calling this (_validate_detection_range_value)
        :param block_size: positive int in range of
        LOWER_BOUND to UPPER_BOUND represent distance block size
        :return: the sql lines represent the separation to different distance ranges
        '''
        output_query=""
        #start,stop,step
        start=SqlQueriesGenerator.LOWER_BOUND
        while start<=SqlQueriesGenerator.UPPER_BOUND:
            end=start+block_size-1
            if end>SqlQueriesGenerator.UPPER_BOUND:#limit to upper bound
                end=SqlQueriesGenerator.UPPER_BOUND
            newline=self._get_sql_detection_range_template().replace("###START###",str(start))
            newline=newline.replace("###END###",str(end))
            start+=block_size#increment to next line
            if start>SqlQueriesGenerator.UPPER_BOUND:
                output_query += newline + "\n"  # append new line (last line)
            else:
                output_query += newline + ",\n"  # append new line
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

    #get range and build the query according to the range ,add line for each range.
    def create_query_by_range(self,block_size:int) ->str:
        '''
        :argument get block size from 1 to 100,
        create a query that contain all the ranges by block size
        :return the new query
        '''
        if self._validate_detection_range_value(block_size)==False:
            logging.error("exit creating sql query ,block size is not correct")
            return None
        lines=self._get_sql_detection_range_lines(block_size)
        query=self._get_template_query().replace("###PLACE_HOLDER###",lines)
        return query
    def _test_create_sql_by_block_sizes(self):
        '''
        see logs or, use debugger ,break point on line " s11="done""
        see generated sql query for each block_size,
        copy the string to sublime/notpad for easy read
        :return: Errors in logs, not returning a value, use for internal debugging
        '''
        s1 = sql_query.create_query_by_range(block_size=11)
        s2 = sql_query.create_query_by_range(block_size=0)#should fail
        s3 = sql_query.create_query_by_range(block_size=100)
        s4 = sql_query.create_query_by_range(block_size=1000000)#should fail
        s5 = sql_query.create_query_by_range(block_size=53)
        s6 = sql_query.create_query_by_range(block_size=99)
        s7 = sql_query.create_query_by_range(block_size=-89)#should fail
        s8 = sql_query.create_query_by_range(block_size="10")#should fail
        s9 = sql_query.create_query_by_range(block_size=1)
        s10 = sql_query.create_query_by_range(block_size=2)
        s11="done"
if __name__ == '__main__':
    #main to test the functions - see logs for the Expected errors
    sql_query=SqlQueriesGenerator()
    #s=sql_query.create_query_by_range(block_size=10)
    sql_query._test_create_sql_by_block_sizes()