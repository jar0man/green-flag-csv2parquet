#!/usr/bin/env python3.7
#
# files_validator - Function utilities to validate files
#
# Copyright (C) 2020 by Alonso Roman <alonso.roman@outlook.com>
#
#
from os.path import isfile, isdir, join, exists
from os import listdir


def is_csv_file(file)-> bool:
    """
    Validate if a file has the extension csv
    :param file: file name
    :return: if the extension is csv or not
    """
    return file.split('.')[-1] == 'csv'


def directory_with_at_least_one_valid(validator_file, location)->bool:
    """
    Validate if in that directory exist the file we are interested in
    :param validator_file: function to validate the file
    :param location: location where files are
    :return: if the directory contains at least one valid file
    """
    return (# it is a directory
            isdir(location) and
            # at least 1 file is valid
            len([file
                         for file in listdir(location)
                          if isfile(join(location, file)) and validator_file(file)]) > 0
             )


def valid_location(validator_file, location)-> bool:
    """
    Validate if in that location exist the file we are interested in
    :param validator_file: function to validate file name
    :param location: folder where files are located or actual file path
    :return: if the file is valid or directory contains at least one valid file
    """
    return validator_file(location) or directory_with_at_least_one_valid(validator_file,location)