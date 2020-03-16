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