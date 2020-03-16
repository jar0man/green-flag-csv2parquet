#!/usr/bin/env python3.7
#
# GFlagLoggerException - Create a exception class for issues related with logger
#
# Copyright (C) 2020 by Alonso Roman <alonso.roman@outlook.com>
#
#


class GFlagLoggerException(Exception):
    """
    Exception class for any exception on our custom Logger class
    """
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return f'GFlagLoggerException, {self.message}'
        else:
            return 'GFlagLoggerException has been raised'
