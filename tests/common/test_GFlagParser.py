#!/usr/bin/env python3.7
#
# test_GFlagParser - Test file for GFlagParser
#
# Copyright (C) 2020 by Alonso Roman <alonso.roman@outlook.com>
#
#

from main.common.GFlagParser import GFlagParser

def test_GFlagParser_default():
    """
    Unit test for Parser, to verify that default values
    are correct and get_item is working fine
    :return:
    """
    parser = GFlagParser()
    assert parser['csv_location'] == './input-data/'
    assert parser['output_location'] == './output.parquet'
    assert parser['logger_location'] == './log/'
    assert parser['output_mode'] == 'overwrite'
    assert parser['logger_level'] == 'info'