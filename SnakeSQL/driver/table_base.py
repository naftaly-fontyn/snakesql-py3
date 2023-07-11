#! python
"""
summary:
    SnakeSQL Py3 table and column base
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

# ###
Table and Column objects

Rationale
---------
By making all the conversion names the same it makes it easier to convert
a loop:
    for i in range(len(columns)):
        table[tableName][i].valueToSQL(values[i])
By having the type as a member
* XXX All SQL conversions should use the properly quoted versions.
Note SQL to value means parsed SQL not raw SQL.

Really need to make proper use of these functions rather than eval() as
implementors of other drivers may have problems
"""

from typing import Union, List
from ..error import (Bug, ConversionError)
# from ..external.tablePrint import table_print
import datetime
# import types
import logging
log = logging.getLogger()


class BaseConverter:
    def __init__(self, col_type: str, SQLQuotes: bool, col_type_code: int):
        self.type = col_type
        self.SQLQuotes = SQLQuotes
        self.typeCode = col_type_code

    def valueToSQL(self, value):
        "Convert a Python object to an SQL string"
        return value

    def sqlToValue(self, value):
        "Convert the an SQL string to a Python object"
        return value

    def storageToValue(self, value):
        "Convert the value stored in the database to a Python object"
        return value

    def valueToStorage(self, value):
        """
        Convert a Python object to the format needed to store it in the
        database
        """
        return value

    def SQLToStorage(self, value):
        """
        Convert a value returned from the SQL Parser to the format needed to
        store it in the database
        """
        return self.valueToStorage(self.sqlToValue(value))


class BaseUnknownConverter(BaseConverter):
    def __init__(self):
        super().__init__(col_type='Unknown', SQLQuotes=False, col_type_code=11)


class BaseStringConverter(BaseConverter):
    def __init__(self, col_type: str = 'String', SQLQuotes: bool = True,
                 col_type_code: int = 5):
        super().__init__(col_type=col_type, SQLQuotes=SQLQuotes,
                         col_type_code=col_type_code)
        self.max = 255

    def storageToValue(self, column):
        return None if column is None else str(column)

    def valueToStorage(self, column):
        if column and len(str(column)) > self.max:
            raise ConversionError(f'Should be {self.max} characters or less.')
        return None if column is None else str(column)

    def valueToSQL(self, column):
        val = self.valueToStorage(column)
        return 'NULL' if val is None else "'{}'".format(
            column.replace("'", "''"))

    def sqlToValue(self, column):
        if column == 'NULL':
            return None
        elif len(str(column)) > self.max+2:
            raise ConversionError(f'Should be {self.max} characters or less.')
        if str(column)[0] != "'" or str(column)[-1:] != "'":
            raise ConversionError(
                f"{self.type} column value {column} should start & end with '")
        return str(column)[1:-1].replace("''", "'")


class BaseTextConverter(BaseStringConverter):
    def __init__(self):
        super().__init__(col_type='Text', SQLQuotes=True, col_type_code=6)
        self.max = 16777215


class BaseBinaryConverter(BaseStringConverter):
    # TODO: should convert to bytes
    def __init__(self):
        super().__init__(col_type='Binary', SQLQuotes=True, col_type_code=7)
        self.max = 16777215


class BaseBoolConverter(BaseConverter):
    def __init__(self):
        super().__init__(col_type='Bool', SQLQuotes=False, col_type_code=1)

    def storageToValue(self, column):
        try:
            return None if column is None else [0, 1][int(column)]
        except ValueError:
            raise ConversionError(
                'Bool columns take the internal values 1/0 not {}, type {}'.
                fromat(column, repr(type(column))[7:-2]))

    def valueToStorage(self, column):
        try:
            return {
                None: None,
                0: 0, False: 0,
                1: 1, True: 1,
                }[column]
        except KeyError:
            raise ConversionError('Bool columns take can only 0/1 not {}'.
                                  format(column))

    def valueToSQL(self, column):
        try:
            return {
                None: 'NULL',
                0: 'FALSE', False: 'FALSE',
                1: 'TRUE', True: 'TRUE'
                }[column]
        except KeyError:
            raise ConversionError('Bool columns take can only be 0/1 not {}'.
                                  format(column))

    def sqlToValue(self, column):
        try:
            return {
                'NULL': None,
                'FALSE': False,
                'TRUE': True,
                }[column.upper()]
        except KeyError:
            raise ConversionError(
                "Bool columns take can only be 'TRUE'/'FALSE' not {}, type {}".
                format(column, repr(type(column))[7:-2]))


class BaseIntegerConverter(BaseConverter):
    # int32
    def __init__(self, col_type: str = 'Integer', SQLQuotes: bool = False,
                 col_type_code: int = 2):
        super().__init__(col_type=col_type, SQLQuotes=SQLQuotes,
                         col_type_code=col_type_code)
        self.max = int(2 ** 31 - 1)
        self.min = -self.max - 1
        self._conv = int

    def storageToValue(self, column):
        if column is None:
            return None
        try:
            i = self._conv(column)
            if ((self.min is None and self.max is None) or
                    self.min <= i <= self.max):
                return i
        except ValueError:
            log.error('column')
            pass
        raise ConversionError("Invalid value {} for {}.".
                              format(column, self.type))

    def valueToStorage(self, column):
        if not isinstance(column, (self._conv, type(None))):
            raise ConversionError("Invalid value %s for Integer column" %
                                  repr(column))
        v = self.storageToValue(column)
        return None if v is None else str(v)

    def valueToSQL(self, column):
        column = self.valueToStorage(column)
        return 'NULL' if column is None else column

    def sqlToValue(self, column):
        return None if column == 'NULL' else self.storageToValue(column)


class BaseLongConverter(BaseIntegerConverter):  # BaseConverter):
    def __init__(self, col_type: str = 'Long', SQLQuotes: bool = False,
                 col_type_code: int = 3):
        super().__init__(col_type=col_type, SQLQuotes=SQLQuotes,
                         col_type_code=col_type_code)
        self.max = int(2 ** 64 - 1)
        self.min = -self.max - 1
        # self._conv = int

    """
    def storageToValue(self, column):
        if column is None:
            return None
        else:
            try:
                i = self._conv(column)
            except ValueError:
                raise ConversionError("Invalid value %s for %s." %
                                      (repr(column), self.type))
            else:
                return i

    def valueToStorage(self, column):
        v = self.storageToValue(column)
        if v is None:
            return None
        else:
            return str(v)

    def valueToSQL(self, column):
        column = self.valueToStorage(column)
        if column is None:
            return 'NULL'
        else:
            return column

    def sqlToValue(self, column):
        if column == 'NULL':
            return None
        else:
            return self.storageToValue(column)
    """


class BaseFloatConverter(BaseLongConverter):
    def __init__(self):
        super().__init__(col_type='Float', SQLQuotes=False, col_type_code=4)
        self.max = self.min = None
        self._conv = float


class BaseDateConverter(BaseConverter):
    def __init__(self):
        super().__init__(col_type='Date', SQLQuotes=True, col_type_code=8)

    def storageToValue(self, column):
        if column is None:
            return None
        sql = str(column)
        try:
            return datetime.date(int(sql[0:4]), int(sql[5:7]), int(sql[8:10]))
        except ValueError:
            raise ConversionError("{} is not a valid Date string.".
                                  format(repr(column)))

    def valueToStorage(self, column):
        return None if column is None else column.isoformat()[:10]

    def valueToSQL(self, column):
        return None if column is None else "'{}'".format(
            str(self.valueToStorage(column)).replace("'", "''"))

    def sqlToValue(self, column):
        if column == 'NULL':
            return None
        if str(column)[0] != "'" or str(column)[-1:] != "'":
            raise ConversionError(
                "{} column value {} should start and end with a ' character.".
                format(self.type, column))
        return self.storageToValue(str(column)[1:-1].replace("''", "'"))


class BaseDatetimeConverter(BaseConverter):
    def __init__(self):
        super().__init__(col_type='DateTime', SQLQuotes=True, col_type_code=9)

    def storageToValue(self, column):
        if column is None:
            return None
        sql = str(column)
        try:
            return datetime.datetime(int(sql[0:4]), int(sql[5:7]),
                                     int(sql[8:10]), int(sql[11:13]),
                                     int(sql[14:16]), int(sql[17:19]))
        except ValueError:
            raise ConversionError("%s is not a valid DateTime string."
                                  % (repr(column)))

    def valueToStorage(self, column):
        return None if column is None else column.isoformat()[:19]

    def valueToSQL(self, column):
        return 'NULL' if column is None else "'{}'".format(
            str(self.valueToStorage(column)).replace("'", "''"))

    def sqlToValue(self, column):
        if column == 'NULL':
            return None
        if str(column)[0] != "'" or str(column)[-1:] != "'":
            raise ConversionError(
                "%s column value %s should start and end with a ' character."
                % (self.type, column))
        return self.storageToValue(str(column)[1:-1].replace("''", "'"))


class BaseTimeConverter(BaseConverter):
    def __init__(self):
        super().__init__(col_type='Time', SQLQuotes=True, col_type_code=10)

    def storageToValue(self, column):
        if column is None:
            return None
        sql = str(column)
        try:
            return datetime.time(int(sql[0:2]), int(sql[3:5]),
                                 int(sql[6:8]))
        except ValueError:
            raise ConversionError("%s is not a valid Time string."
                                  % (repr(column)))

    def valueToStorage(self, column):
        return None if column is None else column.isoformat()[:8]

    def valueToSQL(self, column):
        return 'NULL' if column is None else "'{}'".format(
            str(self.valueToStorage(column)).replace("'", "''"))

    def sqlToValue(self, column):
        if column == 'NULL':
            return None
        if str(column)[0] != "'" or str(column)[-1:] != "'":
            raise ConversionError(
                "%s column value %s should start and end with a ' character."
                % (self.type, column))
        return self.storageToValue(str(column)[1:-1].replace("''", "'"))


class BaseColumn:
    def __init__(self, table: 'BaseTable', name: str, col_type: str,
                 required: bool, unique: bool, primaryKey: bool,
                 foreignKey: str, default: str, converter: BaseConverter,
                 position: int):
        self.name = name
        self.type = col_type
        self.table = table
        self.required = required
        self.unique = unique
        self.primaryKey = primaryKey
        self.foreignKey = foreignKey
        self.default = default
        self.converter = converter
        self.position = position

    def get(self, columnName):  # TODO: BUG?
        for column in self.columns:
            if column.name == columnName:
                return column
        raise KeyError(f"Column {columnName} not found.")


class BaseTable:
    def __init__(self, name: str, filename: Union[str, None] = None,
                 file=None, columns: List[BaseColumn] = []):
        self.name = name
        self.file = file  # XXX
        self.columns = columns
        self.filename = filename
        self.open = False
        self.primaryKey = None
        self.parentTables = []
        self.childTables = []

    def __repr__(self):
        return "<Table %s>" % self.name

    def has_key(self, key):
        return self.columnExists(key)

    def columnExists(self, columnName):
        for column in self.columns:
            if column.name == columnName:
                return True
        return False

    def get(self, columnName):
        for column in self.columns:
            if column.name == columnName:
                return column
        raise Bug("Column %s not found in table %s" %
                  (repr(columnName), repr(self.name)))

    def __getitem__(self, name):
        if isinstance(name, int):
            return self.columns[name]
        else:
            return self.get(name)

    def _load(self):
        self.open = True
        raise Exception("Should be implemented in derived class.")

    def _close(self):
        self.open = False
        raise Exception("Should be implemented in derived class.")

    def commit(self):
        raise Exception("Should be implemented in derived class.")

    def rollback(self):
        raise Exception("Should be implemented in derived class.")
