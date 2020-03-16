# Green Flag Interview Test


Convert the weather data into parquet format. Set the raw group to appropriate value you see fit for this data. 
The converted data should be queryable to answer the following question. 
  - Which date was the hottest day? 
  - What was the temperature on that day? 
  - In which region was the hottest day? 
Please provide the source code, tests, documentations and any assumptions you have made.
 
Note: We are looking for the candidate’s “Data Engineering” ability not just the Python programming skills. 
The weather data is provided separately

Assumptions:
  + We assume that the query to get the hottest day will be performed by year,
  so, we can do an improvement to performance partitioning data by year.
  + We are not interested in the rest of values, so we can eliminate them as
  part of our process
  + Schema is fixed, all the csv files will have the same schema, so, we don't 
  need to infer the schema, which will improve performance and reduce prone errors
  + We are running a spark cluster for our data pipeline
  
### Installing
To make it run, please clone the git and then do the following steps:
1. create an virtual environment running:
    python3 -m venv ./venv
2. activate the virtual environment
    . venv/bin/activate
3. install requirements
    pip3 install -r requirements.txt
4. run unit tests 
    pytest
    
### Running the application    
1. in order to run the application, you need to setup the parameters or run
with the default values. The log, by default will be saved into the folder ./log,
so, you need to create this folder or specify a new folder, which exists already,
with the command line parameter --logger_location 
     mkdir ./log
2. run the application
     python3 -m main.csv2parquet 

### Parameters available
Parameters [default values]:
        + csv_location: file or folder for our csv files  [./input-data/]
        + output_location: location for our output file in parquet  [./output.parquet]
        + logger_location: location where the log will be created in [./log/]
        + output_mode: writer mode [overwrite]
        + logger_level: logger level: info, debug, warning, error [info]

