#!/usr/bin/env python3.7
#
# test_csv2_parquet - Testing functions for csv2_parquet program
#
# Copyright (C) 2020 by Alonso Roman <alonso.roman@outlook.com>
#
#
from pandas.testing import assert_frame_equal
from datetime import datetime

import pytest
from pyspark.sql.types import Row
import pandas as pd
from os.path import join
import logging

from main.csv2parquet import transform, weather_schema, read_weather_csv, saving_weather_as_parquet
logger = logging.getLogger('test_csv2parquet')

@pytest.mark.usefixtures("spark")
def test_transform_happy_path(spark):
    df_test_case = spark.createDataFrame([
        Row(ForecastSiteCode=3008,
            ObservationTime=1,
            ObservationDate=datetime(2018, 12, 1, 4, 15, 0),
            WindDirection=12,
            WindSpeed=2,
            WindGust=37,
            Visibility=20000,
            ScreenTemperature=2.8,
            Pressure=998,
            SignificantWeatherCode=11,
            SiteName='FAIR ISLE (3008)',
            Latitude=59.53,
            Longitude=-1.63,
            Region='Orkney & Shetland',
            Country='SCOTLAND'
            ),
        Row(ForecastSiteCode=3005,
            ObservationTime=2,
            ObservationDate=datetime(2019, 12, 1, 4, 15, 0),
            WindDirection=13,
            WindSpeed=1,
            WindGust=34,
            Visibility=30000,
            ScreenTemperature=5.8,
            Pressure=997,
            SignificantWeatherCode=11,
            SiteName= 'LERWICK (S. SCREEN) (3005)',
            Latitude=59.53,
            Longitude=-1.63,
            Region='Highland & Eilean Siar',
            Country='IRELAND')])

    expected = pd.DataFrame([[ datetime(2018, 12, 1, 4, 15, 0),2.8,'Orkney & Shetland',2018],
                            [datetime(2019, 12, 1, 4, 15, 0),5.8,'Highland & Eilean Siar',2019]]
                            ,columns=['ObservationDate','ScreenTemperature','Region','Year'])

    result = transform( df_test_case, logger).toPandas()
    assert_frame_equal(result, expected,check_dtype=False)


def test_transform_no_dataframe(spark):
    df_test_case = spark.createDataFrame([],weather_schema())
    expected = pd.DataFrame([],columns=['ObservationDate','ScreenTemperature','Region','Year'])

    result = transform(df_test_case, logger).toPandas()

    assert_frame_equal(result, expected, check_dtype=False)


def test_read_weather_csv(spark):

    expected = pd.DataFrame([[3002, 0, datetime(2016, 2, 1, 0, 0, 0), 12, 8,
                              None, 30000, 2.10, 997, 8, 'BALTASOUND (3002)',
                              60.7490, -0.8540, 'Orkney & Shetland', 'SCOTLAND'],
                              [3005, 0, datetime(2017, 2, 2, 0, 0, 0), 10, 2,None ,
                               35000, 0.10, 997, 7, 'LERWICK (S. SCREEN) (3005)',
                               60.1390, -1.1830, 'Highland & Eilean Siar', 'SCOTLAND']]
                              ,columns=['ForecastSiteCode','ObservationTime','ObservationDate',
                                        'WindDirection','WindSpeed','WindGust','Visibility',
                                        'ScreenTemperature','Pressure','SignificantWeatherCode',
                                        'SiteName','Latitude','Longitude','Region','Country']
                            )

    result = read_weather_csv(spark, './tests/resources/test-input-csv-files/', weather_schema(), logger).toPandas()

    assert_frame_equal(result, expected, check_dtype=False)

@pytest.mark.usefixtures("tmp_dir")
def test_saving_weather_as_parquet(spark,tmp_dir):

    test_output_folder = tmp_dir
    test_output_file = join(test_output_folder,'parquet-file')

    test_df_day_temperature = spark.createDataFrame([Row(ObservationDate=datetime(2018, 12, 1, 4, 15, 0),
                                                     ScreentTemperature=2.8, Region='Orkney & Shetland',Year=2018),
                                                     Row(ObservationDate=datetime(2019, 12, 1, 4, 15, 0),
                                                     ScreentTemperature=5.8, Region='Highland & Eilean Siar',Year=2019),
                                                    ])

    saving_weather_as_parquet(test_df_day_temperature,'overwrite',test_output_file, logger)

    # read the result to compare
    result = (spark.read
              .format('parquet')
              .load(test_output_file)
             ).toPandas().sort_values(by=['ObservationDate']).reset_index(drop=True)

    expected = test_df_day_temperature.toPandas().sort_values(by=['ObservationDate']).reset_index(drop=True)
    assert_frame_equal(result, expected, check_dtype=False)


