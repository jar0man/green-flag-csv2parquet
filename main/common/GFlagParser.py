#!/usr/bin/env python3.7
#
# GFlagParser - Create a parser class for an application
#
# Copyright (C) 2020 by Alonso Roman <alonso.roman@outlook.com>
#
#
import argparse


class GFlagParser:
    """
    Custom parser class. This class will allow to grab every parameter
    used by our application.
    Parameters:
        + csv_location: file or folder for our csv files
        + output_location: location for our output file in parquet
        + logger_location: location where the log will be created in
        + output_mode: writer mode
        + logger_level: logger level: info, debug, warning, error
    """
    __args: dict

    def __init__(self):
        parser = argparse.ArgumentParser(description='gf_data_engineer')
        parser.add_argument('--csv_location', required=False, default="./input-data/")
        parser.add_argument('--output_location', required=False, default="./output.parquet")
        parser.add_argument('--logger_location', required=False, default="./log/")
        parser.add_argument('--output_mode', required=False, default="overwrite")
        parser.add_argument('--logger_level', required=False, default='info')

        self.__args = vars(parser.parse_args())

    def __getitem__(self, key):
        if key not in self.__args.keys():
            raise IndexError()
        return self.__args[key]
