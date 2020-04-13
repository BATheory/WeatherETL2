# WeatherETL2

# The Assignment
Convert the weather data into parquet format. Set the raw group to appropriate value you see fit for this data. 
The converted data should be queryable to answer the following question. 
- Which date was the hottest day? 
- What was the temperature on that day? 
- In which region was the hottest day? 


The weather data is provided separately

#Approach to the task

My approach is to convert the csv fie into a data frame and then to Parquet.  Apache Spark will be used to convert and query the data using SQL. 
The highest temperature is based on the highest temperature recorded in the Screen_Temperature and the corresponding region and date returned.
Having done some data understanding before I started the exercise, the orginal csv file contains error readings in the temperature field (-99) so if the business would like to have the highest reading using Statistical techniques these would need to be filtered at the point of querying or removed at the source.
There are three main folders containing the code:
- jobs - this has the main script - etl_jobs.py
- dependencies - has the class/ funtions supporting the jobs file - util.py
- tests - contains the test cases for all cases 
    - test_etl_jobs.py 
    - test_util.py
    - within the tests folder there is another folder called test-data which contains the two csv source files for the test cases.
    

#Programme
This code was constructed and performed on pyChum which uses Apache Spark.

#Execution options
      $SPARK_HOME/bin/spark-submit \
      --master yarn \
      --py-files dependencies.zip \
      etl_job.py
      --weather_files_dir = weather file1 path with csv files
      --output_path =  output path where the parquet fles needs to be stored
     
