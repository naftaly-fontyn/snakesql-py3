"""
"""

import os
import sys
# from .cursor_base import Cursor
from .table_base import (BaseTable, BaseColumn,
                         BaseUnknownConverter, BaseStringConverter,
                         BaseTextConverter, BaseBinaryConverter,
                         BaseBoolConverter, BaseIntegerConverter,
                         BaseLongConverter, BaseFloatConverter,
                         BaseDateConverter, BaseDatetimeConverter,
                         BaseTimeConverter)
from .connection_base import BaseConnection
from ..external import lockdbm
from ..error import Error, Bug


class DBMTable(BaseTable):
    def _load(self):
        self.file = lockdbm.open(self.filename)
        self.open = True

    def _close(self):
        self.file.close()
        self.open = False

    def commit(self):
        self.file.commit()

    def rollback(self):
        self.file.rollback()


class DBMConnection(BaseConnection):
    def __init__(self, database, driver, autoCreate, colTypesName):
        self._closed = None
        self.tableExtensions = ['.dir', '.dat', '.bak']
        super().__init__(database=database, driver=driver,
                         autoCreate=autoCreate, colTypesName=colTypesName)
        self._closed = False

    # Useful methods
    def databaseExists(self):
        "Return True if the database exists, False otherwise."
        if self._closed:
            raise Error('The connection to the database has been closed.')
        base_path = os.path.join(self.database, self.colTypesName)
        if (os.path.exists(self.database) and
                os.path.exists(base_path + '.dir') and
                os.path.exists(base_path + '.dat') and
                os.path.exists(base_path + '.bak')):
            return True
        else:
            return False

    def _deleteTableFromDisk(self, table):
        if self._closed:
            raise Error('The connection to the database has been closed.')
        for end in self.tableExtensions:
            # if os.path.exists(self.database+os.sep+table+end):
            os.remove(self.database+os.sep+table+end)

    def _insertRow(self, table, primaryKey, values, types=None):
        if self._closed:
            raise Error('The connection to the database has been closed.')
        try:
            self.tables[table].file[str(primaryKey)] = str(values)
        except Exception as e:
            print(e)
            raise Bug('Key %s already exists in table %s' %
                      (repr(str(primaryKey)), repr(table)))
        """
        if self.tables[table].file.has_key(str(primaryKey)):
            raise Bug('Key %s already exists in table %s' %
                      (repr(str(primaryKey)), repr(table)))
        self.tables[table].file[str(primaryKey)] = str(values)
        """

    def _deleteRow(self, table, primaryKey):
        if self._closed:
            raise Error('The connection to the database has been closed.')
        del self.tables[table].file[primaryKey]

    def _getRow(self, table, primaryKey):
        if self._closed:
            raise Error('The connection to the database has been closed.')
        # if not self.tables[table].file.has_key(str(primaryKey)):
        try:
            return eval(self.tables[table].file[primaryKey])
        except Exception as e:
            print(e)
            raise Bug('No such key %s exists in table %s' %
                      (repr(str(primaryKey)), repr(table)))
        """
        if primaryKey is not self.tables[table].file:
            print(primaryKey, [k for k in self.tables[table].file])
            raise Bug('No such key %s exists in table %s' %
                      (repr(str(primaryKey)), repr(table)))
        row = self.tables[table].file[str(primaryKey)]
        return eval(row)
        """

    def _updateRow(self, table, oldkey, newkey, values):
        if self._closed:
            raise Error('The connection to the database has been closed.')
        if newkey is None:
            newkey = oldkey
        del self.tables[table].file[oldkey]  # XXX Is this a bug in dumbdbm?
        try:
            self.tables[table].file.has_key(newkey)
            raise Bug("The table %s already has a PRIMARY KEY named %s. This "
                      "error should have been caught earlier." %
                      (repr(table), repr(newkey)))
        except Exception:
            pass
            # raise
        """
        if self.tables[table].file.has_key(newkey):
            raise Bug("The table %s already has a PRIMARY KEY named %s. This "
                      "error should have been caught earlier." %
                      (repr(table) ,repr(newkey)))
        """
        self.tables[table].file[newkey] = str(values)
        return values


driver = {
    'converters': {
        'Unknown':  BaseUnknownConverter(),
        'String':   BaseStringConverter(),
        'Text':     BaseTextConverter(),
        'Binary':   BaseBinaryConverter(),
        'Bool':     BaseBoolConverter(),
        'Integer':  BaseIntegerConverter(),
        'Long':     BaseLongConverter(),
        'Float':    BaseFloatConverter(),
        'Date':     BaseDateConverter(),
        'Datetime': BaseDatetimeConverter(),  # Decision already made.
        'Time':     BaseTimeConverter(),
    },
    'Table': DBMTable,
    'Column': BaseColumn,
    'Connection': DBMConnection,
}