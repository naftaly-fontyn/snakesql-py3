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

# import os
# import sys
from ..error import (Bug, ConversionError)
# from ..external.tablePrint import table_print
import datetime
# import types
import logging
# from ..external import SQLParserTools
# import dtuple
log = logging.getLogger()


class BaseTable:
    def __init__(self, name, filename=None, file=None, columns=[]):
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


class BaseColumn:
    def __init__(self, table, name, type, required, unique, primaryKey,
                 foreignKey, default, converter, position):
        self.name = name
        self.type = type
        self.table = table
        self.required = required
        self.unique = unique
        self.primaryKey = primaryKey
        self.foreignKey = foreignKey
        self.default = default
        self.converter = converter
        self.position = position

    def get(self, columnName):
        for column in self.columns:
            if column.name == columnName:
                return column
        raise Exception("Column %s not found." % (repr(columnName)))


class BaseConverter:
    def __init__(self):
        pass  # self.SQLQuotes = False     #self.typeCode = None

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
        a = BaseConverter.__init__(self)
        self.type = 'Unknown'  # Column type should be specified in definition
        self.SQLQuotes = False
        self.typeCode = 11
        return a


class BaseStringConverter(BaseConverter):
    def __init__(self):
        a = BaseConverter.__init__(self)
        self.type = 'String'  # Column type should be specified in definition
        self.SQLQuotes = True
        self.max = 255
        self.typeCode = 5
        return a

    def storageToValue(self, column):
        if column is None:
            return None
        else:
            return str(column)

    def valueToStorage(self, column):
        if column is None:
            return None  # Note, not NULL
        if len(str(column)) > self.max:
            raise ConversionError('Should be %s characters or less.' %
                                  self.max)
        return str(column)

    def valueToSQL(self, column):
        if column is None:
            return 'NULL'
        elif len(str(column)) > self.max:
            raise ConversionError('Should be %s characters or less.' %
                                  self.max)
        return "'" + str(column).replace("'", "''") + "'"

    def sqlToValue(self, column):
        if column == 'NULL':
            return None
        elif len(str(column)) > self.max+2:
            raise ConversionError('Should be %s characters or less.' %
                                  self.max)
        if str(column)[0] != "'" or str(column)[-1:] != "'":
            raise ConversionError(
                "%s column value %s should start and end with a ' character."
                % (self.type, column))
        return str(column)[1:-1].replace("''", "'")


class BaseTextConverter(BaseStringConverter):
    def __init__(self):
        a = BaseStringConverter.__init__(self)
        self.type = 'Text'  # Column type should be specified in definition
        self.SQLQuotes = True
        self.max = 16777215
        self.typeCode = 6
        return a


class BaseBinaryConverter(BaseStringConverter):
    # XXX Not sure how this is going to work!
    def __init__(self):
        a = BaseStringConverter.__init__(self)
        self.type = 'Binary'  # Column type should be specified in definition
        self.SQLQuotes = True
        self.max = 16777215
        self.typeCode = 7
        return a


class BaseBoolConverter(BaseConverter):
    def __init__(self):
        a = BaseConverter.__init__(self)
        self.type = 'Bool'  # Column type should be specified in definition
        self.SQLQuotes = False
        self.typeCode = 1
        return a

    def storageToValue(self, column):
        if column is None:
            return None
        elif column in [1, 0]:
            return int(column)
        else:
            raise ConversionError(
                'Bool columns take the internal values 1 or 0 not %s, type %s'
                % (column, repr(type(column))[7:-2]))

    def valueToStorage(self, column):
        if column is None:
            return None
        elif column in [1, True]:
            return 1
        elif column in [0, False]:
            return 0
        else:
            raise ConversionError('Bool columns take can only be 1 or 0 not %s'
                                  % (column))

    def valueToSQL(self, column):
        if column is None:
            return 'NULL'
        elif column in [1, True]:
            return 'TRUE'
        elif column in [0, False]:
            return 'FALSE'
        else:
            raise ConversionError('Bool columns take can only be 1 or 0 not %s'
                                  % (column))

    def sqlToValue(self, column):
        if column == 'NULL':
            return None
        elif str(column).upper() == 'TRUE':
            return True
        elif str(column).upper() == 'FALSE':
            return False
        else:
            raise ConversionError(
                "Bool columns take can only be 'TRUE' or 'FALSE' not %s, "
                "type %s" % (column, repr(type(column))[7:-2]))


class BaseIntegerConverter(BaseConverter):
    def __init__(self):
        a = BaseConverter.__init__(self)
        self.type = 'Integer'  # Column type should be specified in definition
        self.max = int(2 ** 31 - 1)
        self.min = -self.max - 1
        self.SQLQuotes = False
        self.typeCode = 2
        return a

    def storageToValue(self, column):
        if column is None:
            return None
        else:
            try:
                i = int(column)
            except ValueError:
                raise ConversionError("Invalid value %s for %s." %
                                      (repr(column), self.type))
            else:
                if i > self.max:
                    raise ConversionError(
                        'Integer too large. Maximum value is %s' % (self.max))
                elif i < self.min:
                    raise ConversionError(
                        'Integer too small. Minimum value is %s' % (self.min))
                else:
                    return i

    def valueToStorage(self, column):
        if not isinstance(column, (int, type(None))):
            raise ConversionError("Invalid value %s for Integer column" %
                                  repr(column))
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


class BaseLongConverter(BaseConverter):
    def __init__(self):
        a = BaseConverter.__init__(self)
        self.type = 'Long'  # Column type should be specified in definition
        self._conv = int  # long
        self.SQLQuotes = False
        self.typeCode = 3
        return a

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


class BaseFloatConverter(BaseLongConverter):
    def __init__(self):
        a = BaseLongConverter.__init__(self)
        self.type = 'Float'  # Column type should be specified in definition
        self._conv = float
        self.SQLQuotes = False
        self.typeCode = 4
        return a


class BaseDateConverter(BaseConverter):
    def __init__(self):
        a = BaseConverter.__init__(self)
        self.type = 'Date'  # Column type should be specified in definition
        self.SQLQuotes = True
        self.typeCode = 8
        return a

    def storageToValue(self, column):
        if column is None:
            return None
        else:
            sql = str(column)
            try:
                return datetime.date(int(sql[0:4]), int(sql[5:7]),
                                     int(sql[8:10]))
            except ValueError:
                raise ConversionError("%s is not a valid Date string." %
                                      (repr(column)))

    def valueToStorage(self, column):
        if column is None:
            return None
        else:
            return column.isoformat()[:10]

    def valueToSQL(self, column):
        if column is None:
            return 'NULL'
        return "'" + str(self.valueToStorage(column)).replace("'", "''") + "'"

    def sqlToValue(self, column):
        if column == 'NULL':
            return None
        if str(column)[0] != "'" or str(column)[-1:] != "'":
            raise ConversionError(
                "%s column value %s should start and end with a ' character."
                % (self.type, column))
        return self.storageToValue(str(column)[1:-1].replace("''", "'"))


class BaseDatetimeConverter(BaseConverter):
    def __init__(self):
        a = BaseConverter.__init__(self)
        self.type = 'DateTime'  # Column type should be specified in definition
        self.SQLQuotes = True
        self.typeCode = 9
        return a

    def storageToValue(self, column):
        if column is None:
            return None
        else:
            sql = str(column)
            try:
                return datetime.datetime(int(sql[0:4]), int(sql[5:7]),
                                         int(sql[8:10]), int(sql[11:13]),
                                         int(sql[14:16]), int(sql[17:19]))
            except ValueError:
                raise ConversionError("%s is not a valid DateTime string."
                                      % (repr(column)))

    def valueToStorage(self, column):
        if column is None:
            return None
        else:
            return column.isoformat()[:19]

    def valueToSQL(self, column):
        if column is None:
            return 'NULL'
        return "'" + str(self.valueToStorage(column)).replace("'", "''") + "'"

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
        a = BaseConverter.__init__(self)
        self.type = 'Time'  # Column type should be specified in definition
        self.SQLQuotes = True
        self.typeCode = 10
        return a

    def storageToValue(self, column):
        if column is None:
            return None
        else:
            sql = str(column)
            try:
                return datetime.time(int(sql[0:2]), int(sql[3:5]),
                                     int(sql[6:8]))
            except ValueError:
                raise ConversionError("%s is not a valid Time string."
                                      % (repr(column)))

    def valueToStorage(self, column):
        if column is None:
            return None
        else:
            return column.isoformat()[:8]

    def valueToSQL(self, column):
        if column is None:
            return 'NULL'
        return "'" + str(self.valueToStorage(column)).replace("'", "''") + "'"

    def sqlToValue(self, column):
        if column == 'NULL':
            return None
        if str(column)[0] != "'" or str(column)[-1:] != "'":
            raise ConversionError(
                "%s column value %s should start and end with a ' character."
                % (self.type, column))
        return self.storageToValue(str(column)[1:-1].replace("''", "'"))
