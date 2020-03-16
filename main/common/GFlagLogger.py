#!/usr/bin/env python3.7
#
# GFlagLogger - Create a parser class for an application
#
# Copyright (C) 2020 by Alonso Roman <alonso.roman@outlook.com>
#
#
import logging
from datetime import datetime
from os import path

from main.common.GFlagLoggerException import GFlagLoggerException

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}


class GFlagLogger():
    """
    Custom logging class. To change logging level, please change setLevel value
    To use it, create a new instance and get the logger attribute as

    logger = GFlagLogger(name_of_your_class).logger
    """
    def __init__(self, location: str, name: str, level: str):
        if level not in LEVELS:
            raise GFlagLoggerException(f'Invalid Level: {level}')
        if not path.isdir(location):
            raise GFlagLoggerException(f'Invalid location: {level}, it must be a valid folder')

        logger_file_name = datetime.now().strftime("%Y%m%d%H-%M-%S") + '_' + name + '.log'
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logging.basicConfig(level=LEVELS[level])
        logger_file_handler = logging.FileHandler(path.join(location,logger_file_name))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger_file_handler.setFormatter(formatter)
        logger.addHandler(logger_file_handler)

        self.logger = logger

