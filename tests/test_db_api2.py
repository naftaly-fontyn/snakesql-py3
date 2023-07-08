#! python
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
log.addHandler(log_stream)


TEST_PATH = os.path.dirname(__file__)

TYPES = ['String', 'Bool', 'Text', 'Integer', 'Float', 'Date', 'DateTime',
         'Time']

SQL_VALUES = ["'str''in''''g2'", 'FaLsE', "'string2'", '2', '1.2',
              "'" + str(datetime.date(2004, 12, 12)) + "'",
              "'" + str(datetime.datetime(2004, 12, 12, 12, 12, 12)) + "'",
              "'" + str(datetime.time(1, 12, 12)) + "'"]

VALUES = ["str'in''g2", 0, "string2", 2, 1.2, datetime.date(2004, 12, 12),
          datetime.datetime(2004, 12, 12, 12, 12, 12),
          datetime.time(1, 12, 12)]


def insertTest(cursor: SnakeSQL.Cursor):
    global TYPES
    sql = "create table tableTest ("
    for type_ in TYPES:
        sql += " column%s %s," % (type_, type_)
    sql = sql[:-1]+")"
    cursor.execute(sql)

    sql = ("INSERT INTO tableTest (column%s) VALUES (%s)" %
           (', column'.join(TYPES), ', '.join(SQL_VALUES)))
    cursor.execute(sql)

    sql = "Select column%s from tableTest" % ', column'.join(TYPES)
    cursor.execute(sql)
    results1 = cursor.fetchall()

    sql = "delete from tableTest"
    cursor.execute(sql)

    sql = ("INSERT INTO tableTest (column%s) VALUES (?,?,?,?,?,?,?,?)" %
           (', column'.join(TYPES)))
    cursor.execute(sql, VALUES)

    sql = "Select column%s from tableTest" % ', column'.join(TYPES)
    cursor.execute(sql)
    results2 = cursor.fetchall()

    if results1 == results2 == (tuple(VALUES),):
        print("PASSED Insert test.")
    else:
        print("FAILED Insert test.")
        print(results2, VALUES)


def updateTest(cursor: SnakeSQL.Cursor):
    sql = "UPDATE tableTest SET "
    for i in range(len(VALUES)):
        sql += 'column' + TYPES[i] + ' = ' + SQL_VALUES[i] + ', '
    sql = sql[:-2]
    cursor.execute(sql)

    sql = "Select column%s from tableTest" % ', column'.join(TYPES)

    cursor.execute(sql)
    results1 = cursor.fetchall()
    # log.info(results1)
    sql = "UPDATE tableTest SET "
    for i in range(len(VALUES)):
        sql += 'column'+TYPES[i] + ' = ? , '
    sql = sql[:-2] + "where columnString <> 'hd'"

    cursor.execute(sql, VALUES)

    sql = "Select column%s from tableTest" % ', column'.join(TYPES)
    cursor.execute(sql)
    results2 = cursor.fetchall()

    # log.debug("update tableTest SET columnString = ?, columnText = ? "
    #           "WHERE columnString = ? and columnText = ?  ")

    cursor.execute("update tableTest SET columnString = ?, columnText = ? "
                   "WHERE columnString = '4' and columnText = '4'",
                   ("str'ing2", "string2"))
    cursor.execute("update tableTest SET columnString = ?, columnText = ? "
                   "WHERE columnString = ? and columnText = ?  ",
                   ("4", "4", "string2", "string2"))
    sql = "Select column%s from tableTest" % ', column'.join(TYPES)
    cursor.execute(sql)
    results3 = cursor.fetchall()
    if results1 == results2 == results3 == (tuple(VALUES),):
        # log.info("PASSED Update test.")
        pass
    else:
        log.debug("FAILED Update test.")
        log.debug(f'\n={results1} \n= {results2} \n= {results3}: \n {VALUES}')
        raise Exception()


def conversionTest():
    tt = round(time.time(), 0)
    td = datetime.datetime.fromtimestamp(tt)

    if SnakeSQL.Date(2004, 12, 12) != datetime.date(2004, 12, 12):
        print("Date failed.", SnakeSQL.Date(2004, 12, 12),
              datetime.date(2004, 12, 12))
    if SnakeSQL.Time(12, 12, 12) != datetime.time(12, 12, 12):
        print("Time failed.")
    if (SnakeSQL.Timestamp(2004, 12, 12, 12, 12, 12) !=
            datetime.datetime(2004, 12, 12, 12, 12, 12)):
        print("Timestamp failed.")
    if SnakeSQL.DateFromTicks(tt) != td.date():
        print("DateFromTicks failed.", SnakeSQL.DateFromTicks(time.time()))
    if SnakeSQL.TimeFromTicks(tt) != td.time():
        print("TimeFromTicks failed.", SnakeSQL.TimeFromTicks(time.time()),
              datetime.datetime.now().time())
    if SnakeSQL.TimestampFromTicks(tt) != td:
        print("TimestampFromTicks failed.",
              SnakeSQL.TimestampFromTicks(time.time()),
              datetime.datetime.now())
    if SnakeSQL.Binary('hello') != 'hello':
        print("Binary failed.")


def typesTest():
    if (not SnakeSQL.STRING == SnakeSQL._type_codes['TEXT'] and
            SnakeSQL.STRING == SnakeSQL._type_codes['STRING']):
        print("STRING comparison failed.")
    if not SnakeSQL.BINARY == SnakeSQL._type_codes['BINARY']:
        print("BINARY comparison failed.",
              SnakeSQL.BINARY.values, SnakeSQL._type_codes['BINARY'])
    if not SnakeSQL.DATE == SnakeSQL._type_codes['DATE']:
        print("DATE comparison failed.")
    if not SnakeSQL.TIME == SnakeSQL._type_codes['TIME']:
        print("TIME comparison failed.")
    if not SnakeSQL.TIMESTAMP == SnakeSQL._type_codes['DATETIME']:
        print("TIMESTAMP comparison failed.")
    if (not SnakeSQL.NUMBER == SnakeSQL._type_codes['INTEGER'] and
            SnakeSQL.NUMBER == SnakeSQL._type_codes['LONG']):
        print("NUMBER comparison failed.")


# START test class
class TestDbApi2(unittest.TestCase):
    """
    def setUp(self):
        super().setUp()
        # remove DB if exists
    def tearDown(self):
        ...
    """

    def test_dumb_dbm(self):
        """
        Test that it can sum a list of integers
        """
        if os.path.exists(os.path.join(TEST_PATH, '_testdbm')):
            shutil.rmtree(os.path.join(TEST_PATH, '_testdbm'))
        connection = SnakeSQL.connect(
            os.path.join(TEST_PATH, '_testdbm'), driver='dbm', autoCreate=True)
        cursor = connection.cursor()
        insertTest(cursor)
        updateTest(cursor)
        connection.commit()
        connection.close()

    def test_csv(self):
        """
        Test that it can sum a list of integers
        """
        if os.path.exists(os.path.join(TEST_PATH, '_testcsv')):
            shutil.rmtree(os.path.join(TEST_PATH, '_testcsv'))
        connection = SnakeSQL.connect(
            os.path.join(TEST_PATH, '_testcsv'), driver='csv', autoCreate=True)
        # log.info(connection)
        cursor = connection.cursor()
        insertTest(cursor)
        updateTest(cursor)
        connection.commit()
        connection.close()

    def test_conversions(self):
        conversionTest()

    def test_types(self):
        typesTest()

    def test_sql_parser(self):
        parser = Transform()
        test_sql = {
            'delete': "DELETE FROM table WHERE one=11 or two=22 or three='2 3'"
            " or four=55 or five=90",
            'update': "UPDATE table SET one='''', two='dfg' WHERE ( ( "
            "one='''''' and ( not two='22' ) or ( not two='22' ) ) )",
            'select': "SELECT one, two FROM table1, table2 WHERE ( "
            "one='NU ORDER BY LL' ) and table1.ty>=' 2 asd 1 ' "
            "ORDER BY column DESC, two",
            'select2': "SELECT one, two FROM table WHERE "
            "( one='NU ORDER BY LL' ) and two>=' 2 asd 1 '",
            'select3': "SELECT * FROM test WHERE keyString='3'",
            'insert': "INSERT INTO table_name1 (column_name1, column_name2)"
            " VALUES ('te,''\nst', ?)",
            'create': "CREATE TABLE table_name1 ""(columnName1 String "
            "REQUIRED UNIQUE PRIMARY KEY, column_name2 Integer DEFAULT=?, "
            "column_name3 Integer FOREIGN KEY=table)",
            'drop': "DROP TABLE tableName, table2",
        }
        for k, s in test_sql.items():
            parsed_sql = parser.parse(s)
            re_build = parser.build(**parsed_sql)
            self.assertEqual(re_build, s)

    def test_curser_interface(self):

        if os.path.exists(os.path.join(TEST_PATH, '_test_cruser')):
            shutil.rmtree(os.path.join(TEST_PATH, '_test_cruser'))

        connection = SnakeSQL.connect(os.path.join(TEST_PATH, '_test_cruser'),
                                      driver='dbm', autoCreate=True)
        cursor = connection.cursor()
        sql = cursor.create(
            table='test',
            columns=[
                cursor.column(name='columnDate', type='Date'),
                cursor.column(name='keyString', type='String', key=True),
                cursor.column(name='columnString', type='String'),
                cursor.column(name='columnText', type='Text'),
                cursor.column(name='requiredText', type='Text', required=True),
                cursor.column(name='columnBool', type='Bool'),
                cursor.column(name='columnInteger', type='Integer'),
                cursor.column(name='uniqueInteger', type='Integer',
                              unique=True),
                cursor.column(name='columnLong', type='Long'),
                cursor.column(name='columnFloat', type='Float'),
                cursor.column(name='columnDateTime', type='DateTime'),
                cursor.column(name='columnTime', type='Time'),
            ],
            execute=False
        )
        # log.info(sql)
        cursor.execute(sql, [5])

        cursor.insert(
            table='test',
            columns=[
                'columnDate',
                'keyString',
                'columnString',
                'columnText',
                'requiredText',
                'columnBool',
                'columnInteger',
                'uniqueInteger',
                'columnLong',
                'columnFloat',
                'columnDateTime',
                'columnTime',
            ],
            values=[
                datetime.date(2004, 12, 12),
                "str''ing1",
                'string3',
                'string4',
                'string2',
                False,
                1,
                2,
                999999999999999999,
                1.2,
                datetime.datetime(2004, 12, 12, 12, 12, 12),
                datetime.time(1, 12, 12),
            ]
        )

        cursor.insert(table='test', columns='keyString',
                      values=["str''ing2"], execute=False)
        cursor.update(table='test', columns='keyString',
                      values=["str''ing2"], where="keyString > '56'",
                      execute=False)
        try:
            cursor.insert(table='test', columns='keyString',
                          values="str''ing2")
        except SQLError:
            # log.exception(str(e))
            # log.info("Caught: " + str(sys.exc_info()[1]))
            pass
        else:
            log.info("FAILED to catch an error")
            self.fail()

        try:
            cursor.insert(table='test', columns='uniqueInteger', values=2)
        except SQLError:
            # log.info(e)
            # log.info("Caught: " + str(sys.exc_info()[1]))
            pass
        else:
            log.error("FAILED to catch an error")
            self.fail()

        try:
            cursor.insert(
                table='test',
                columns=[
                    'keyString',
                    'uniqueInteger',
                    ],
                values=[
                    'string2',
                    2,
                    ]
                )
        except SQLError:
            pass
        else:
            log.error("FAILED to catch an error")
            self.fail()

        try:
            cursor.insert(
                table='test',
                columns=[
                    'keyString',
                    'uniqueInteger',
                ],
                values=[
                    'string2',
                    2,
                ]
            )
        except SQLError:
            pass
        else:
            self.fail("FAILED to catch an error")

        try:
            cursor.insert(
                table='test',
                columns=[
                    'keyString',
                    'uniqueInteger',
                    'requiredText',
                ],
                values=[
                    'string2',
                    2,
                    None,
                ]
            )
        except SQLError:
            pass
        else:
            self.fail("FAILED to catch an error")

        try:
            cursor.insert(
                table='test',
                columns=[
                    'keyString',
                    'uniqueInteger',
                    'requiredText',
                ],
                values=['string2', 2, 'Text']
            )
        except SQLError:
            pass
        else:
            self.fail("FAILED to catch an error")

        cursor.insert(
            table='test',
            columns=[
                'keyString',
                'uniqueInteger',
                'requiredText',
            ],
            values=['string2', 3, 'Text']
        )

        cursor.select(
            tables='test',
            columns='*',
        )

        try:
            cursor.update(
                table='test',
                columns='uniqueInteger',
                values=2,
                where="keyString <> NULL",  # Not good.
            )
        except SQLError:
            pass
        else:
            self.fail()

        cursor.update(
            table='test',
            columns='keyString',
            values="str''ing2",
            where="keyString <> NULL",  # Not good.
        )

        try:
            cursor.update(
                table='test',
                columns='uniqueInteger',
                values=3,
                where="uniqueInteger = 2",  # Not good.
            )
        except SQLError:
            pass
        else:
            self.fail("FAILED to catch an error")

        cursor.update(
            table='test',
            columns='requiredText',
            values='newtext',
            where="uniqueInteger = 2",  # Not good.
        )

        cursor.select(
            tables='test',
            columns=[
                'columnDate',
                'keyString',
                'columnDateTime',
                'columnString',
                'columnText',
                'requiredText',
                'columnBool',
                'columnInteger',
                'uniqueInteger',
                'columnLong',
                'columnFloat',
                'columnTime',
            ],
        )

        cursor.delete(
            table='test',
            where="keyString='string2'",  # Not good.
        )

        cursor.select(
                tables='test',
                columns='*',
        )

        cursor.update(
            table='test',
            columns=[
                'columnDate',
                'columnString',
                'columnText',
                'columnBool',
                'columnInteger',
                'columnLong',
                'columnFloat',
                'columnDateTime',
                'columnTime',
            ],
            values=[
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ]
        )

        cursor.select(
            tables='test',
            columns='*',
            where="requiredText='newtext'"
        )

        cursor.drop(
            tables=['test'],
        )

        try:
            log.debug(cursor.select(
                tables='test',
                columns='*',
            ))
        except SQLError:
            pass
        else:
            self.fail("FAILED to catch an error")

        sql = ("SELECT one, two FROM table WHERE ( one='NU ORDER BY LL' ) and "
               "two>=' 2 asd 1 ' ORDER BY column DESC, two")
        blocks = Parser().parse(sql)
        del blocks['function']
        blocks['execute'] = False
        if cursor.select(**blocks) != sql:
            self.fail("FAILED")

        sql = ("INSERT INTO table_name1 (column_name1, column_name2) VALUES "
               "('te,''\nst', '546')")
        blocks = Parser().parse(sql)
        del blocks['function']
        blocks['execute'] = False
        if cursor.insert(**blocks) != sql:
            self.fail("FAILED")

        sql = "UPDATE table SET one='''', two='dfg'"
        blocks = Parser().parse(sql)
        del blocks['function']
        blocks['execute'] = False
        if cursor.update(**blocks) != sql:
            self.fail("FAILED")

        sql = ("CREATE TABLE table_name1 (columnName1 String REQUIRED UNIQUE "
               "PRIMARY KEY, column_name2 Integer DEFAULT='4''')")
        blocks = Parser().parse(sql)
        del blocks['function']
        blocks['execute'] = False
        self.assertEqual(cursor.create(**blocks), sql)

        sql = ("DELETE FROM table WHERE one=11 or two=22 or three='2 3' or "
               "four=55 or five=90")
        blocks = Parser().parse(sql)
        del blocks['function']
        blocks['execute'] = False
        self.assertEqual(cursor.delete(**blocks), sql)

        sql = "DROP TABLE tableName"
        blocks = Parser().parse(sql)
        del blocks['function']
        blocks['execute'] = False
        self.assertEqual(cursor.drop(**blocks), sql)

        log.info('Warning: cursor.update() and \ncursor.insert() not tested '
                 'for execute="False" with real values')
        connection.commit()


if __name__ == '__main__':
    print('Cleanup first')
    unittest.main()
