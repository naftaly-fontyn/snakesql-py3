#! python
# -*- coding: utf-8 -*-
"""
summary:
    SnakeSQL Py3 basic test
Usage:


description:

:REQUIRES:

:TODO:

:AUTHOR:        $Author: Naftaly$
:ORGANIZATION:  N/A
:CONTACT:       [TBD]
:LAST_MODIFIED: $Date$
:Id:            $Id$
:REVISION:      $Tag$

"""

import os
import sys
import time
import shutil
import logging
import datetime
import unittest
import SnakeSQL
from SnakeSQL.external.SQLParserTools import Transform, Parser
from SnakeSQL.error import SQLError


log = logging.getLogger()
# logging.basicConfig()  # stream=sys.stderr)
log_stream = logging.StreamHandler(sys.stderr)
log.setLevel(logging.DEBUG)
log_stream_fmt = logging.Formatter(
    '%(levelname)s:%(filename)s[%(lineno)s]: %(message)s')
log_stream.setFormatter(log_stream_fmt)
# log.addHandler(log_stream)


TEST_PATH = os.path.dirname(__file__)


class TestSql(unittest.TestCase):
    """
    def setUp(self):
        super().setUp()
        # remove DB if exists
    def tearDown(self):
        ...
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if os.path.exists(os.path.join(TEST_PATH, '_testSql')):
            shutil.rmtree(os.path.join(TEST_PATH, '_testSql'))
        connection = SnakeSQL.connect(
            os.path.join(TEST_PATH, '_testSql'), driver='csv', autoCreate=True)
        cursor = connection.cursor()
        sql = "".join(["create table tableSql (",
                       "keyInteger Integer Primary Key,",
                       "uniqueInteger Integer unique,",
                       "requiredText Text required,",
                       "columnText Text",
                       ")",
                       ])
        cursor.execute(sql)
        sql = "".join([
            "INSERT INTO tableSql (",
            "keyInteger,"
            "requiredText,"
            "columnText",
            ") ",
            "VALUES (",
            "0,",
            "'Must',"
            "'this_is a string'",
            ")",
            ])
        # log.info(sql)
        cursor.execute(sql)
        connection.commit()
        cls.connection = connection

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.connection.close()

    def test_like(self):
        cursor = self.connection.cursor()
        sql = ("SELECT columnText FROM tableSql "
               "WHERE tableSql.columnText LIKE '%this_is a st%'")
        cursor.execute(sql)
        # log.info(cursor.info)
        # log.info(cursor.rowcount)
        self.assertEqual(1, cursor.rowcount)
        log.info(cursor.description)
        log.info(cursor.fetchall(format='dict'))
