#!/usr/bin/env python3.7
#
# test_GFlagLogger - Test file for GFlagLogger
#
# Copyright (C) 2020 by Alonso Roman <alonso.roman@outlook.com>
#
#

import pytest

from main.common.GFlagLoggerException import GFlagLoggerException
from main.common.GFlagLogger import GFlagLogger

test_location = './test_log'

@pytest.mark.usefixtures("tmp_dir")
def test_creation_logger_invalid_level(tmp_dir):
    with pytest.raises(GFlagLoggerException):
        GFlagLogger(tmp_dir,'test_gflag_logger','fail').logger


def test_creation_logger_invalid_location():
    with pytest.raises(GFlagLoggerException):
        GFlagLogger(test_location,'test_gflag_logger','info').logger
