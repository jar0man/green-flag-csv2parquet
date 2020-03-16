#!/usr/bin/env python3.7
#
# csv2parquet - Transform a csv file into parquet
#
# Copyright (C) 2020 by Alonso Roman <alonso.roman@outlook.com>
#
#

from main.common.files_validator import is_csv_file, valid_location

from pyspark.sql import SparkSession, types, DataFrame
from pyspark.sql.functions import col, year
from main.common.GFlagParser import GFlagParser
from main.common.GFlagLogger import GFlagLogger

args = GFlagParser()
logger = GFlagLogger(args['logger_location'],'csv2parquet',args['logger_level']).logger


def weather_schema():
    """
    Schema of the CSV file containing the weather information
    :return: schema of the csv file
    """
    schema = types.StructType([
        types.StructField('ForecastSiteCode', types.IntegerType(),True),
        types.StructField('ObservationTime', types.IntegerType(),True),
        types.StructField('ObservationDate', types.TimestampType(),False),
        types.StructField('WindDirection', types.IntegerType(),True),
        types.StructField('WindSpeed', types.IntegerType(),True),
        types.StructField('WindGust', types.IntegerType(),True),
        types.StructField('Visibility', types.IntegerType(),True),
        types.StructField('ScreenTemperature', types.DoubleType(),False),
        types.StructField('Pressure', types.IntegerType(),True),
        types.StructField('SignificantWeatherCode', types.IntegerType(),True),
        types.StructField('SiteName', types.StringType(),True),
        types.StructField('Latitude', types.DoubleType(),True),
        types.StructField('Longitude', types.DoubleType(),True),
        types.StructField('Region', types.StringType(),False),
        types.StructField('Country', types.StringType(),True),
    ])
    return schema


def transform(weather_info: DataFrame) -> DataFrame:
    """
     Function to extract from the weather information dataframe
     only the relevant information to get the day, temperature and region
    :param weather_info: data frame with weather information
    :return: A dataframe with a projection of the following columns:
        ObservationDate, ScreenTemperature, Region, Year (new column added)
    """
    logger.info(f'Transforming Dataframe with {weather_info.count()} rows')

    return (weather_info.select("ObservationDate", "ScreenTemperature", "Region")
                     .withColumn("Year", year(col('ObservationDate')))
                     )


def read_weather_csv(spark, csv_location, schema ) -> DataFrame:
    """
    Read the csv with the weather schema and return a data frame
    containing the data
    :param spark: spark session
    :param csv_location: location of csv file or folder with csv files
    :param schema: csv schema
    :return: Data Frame containing csv data
    """
    logger.info(f"loading csv files from {csv_location}")
    assert valid_location(is_csv_file, csv_location), "There are not CSV files in the input location"
    return (spark.read
            .format('csv')
            .option('header', True)
            .schema(schema)
            .load(csv_location)
            )


def saving_weather_as_parquet(df_day_temperature:DataFrame , outputmode: str, outputlocation: str):
    """
    Function to save the data frame as parquet
    :param df_day_temperature: dataframe containing the following columns:
           ObservationDate, ScreenTemperature, Region, Year
    :param outputmode: indicate if the function is going to overwrite or append data
    :param outputlocation: the path where the file is going to be saved into
    :return: None
    """
    # Not exist and we want to create or exist and we are appending
    logger.info(f'Saving files into {args["output_location"]}')
    # Saving as parquet
    (df_day_temperature.write
     .format('parquet')
     .partitionBy('Year')
     .mode(outputmode)
     .save(outputlocation)
     )


if __name__ == "__main__":

    # Starting Spark
    logger.info('Starting Spark...')
    spark_session = (
        SparkSession.builder
            .master("local[2]")
            .appName("Csv2Parquet")
            .getOrCreate()
    )

    logger.info('Spark started')

    # Read using the schema, as it is better performance and prevent issues
    df_weather = read_weather_csv(spark_session, args['csv_location'],weather_schema())

    df_day_temperature = transform(df_weather)

    # Saving as parquet
    saving_weather_as_parquet(df_day_temperature,args['output_mode'],args['output_location'])

    spark_session.stop()

