"""Table and Column objects

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

from ..error import (Bug, ConversionError, DatabaseError, Error, SQLError,
                     ConverterError, CorruptionError, InternalError,
                     SQLSyntaxError, SQLForeignKeyError, SQLKeyError)
import sys
import os
from typing import Union, List
import logging
from ..external import SQLParserTools
from .cursor_base import Cursor, _raise_closed
# import dtuple
log = logging.getLogger()


def like(value, sql):
    import re
    sql = sql.replace(
        '\\', '\\\\').replace('*', '\\*').replace('%', '*')
    if sql[0] == '*':
        if re.search(sql[1:], value):
            return 1
        else:
            return 0
    else:
        if re.match(sql, value):
            return 1
        else:
            return 0


# def _raise_closed(func):
#     def _wrap(self_, *argv, **kwarg):
#         if self_._closed:
#             raise Error(
#                 'The connection to the database has already been closed.')
#         return func(self_, *argv, **kwarg)
#     return _wrap


class BaseConnection:
    # Other Methods
    def __init__(self, database: str, driver: str, autoCreate: bool,
                 colTypesName: str):
        # todo: change colTypesName to master_table_name
        self.database = database
        self.driver = driver
        self.colTypesName = colTypesName
        self.tables = {}
        self.parser = SQLParserTools.Transform()
        self.createdTables = []
        if not self.databaseExists():
            if autoCreate:
                self.createDatabase()
            else:
                raise DatabaseError(f"The database '{self.database}' "
                                    "does not exist.")
        self._loadTableStructure()

    def __del__(self):
        if self._closed is False:
            self.close()

    # DB-API 2.0 Methods
    def close(self):
        # XXX This should stop everything else from working but currently
        # doesn't!
        """
        Close the connection now (rather than whenever __del__ is
        called).  The connection will be unusable from this point
        forward; an Error (or subclass) exception will be raised
        if any operation is attempted with the connection. The
        same applies to all cursor objects trying to use the
        connection.  Note that closing a connection without
        committing the changes first will cause an implicit
        rollback to be performed.
        """
        if self._closed:
            raise Error(
                'The connection to the database has already been closed.')
        self.rollback()
        for table in self.tables.keys():
            if self.tables[table].open:
                self.tables[table]._close()
        self._closed = True

    @_raise_closed
    def commit(self):
        """
        Commit any pending transaction to the database. Note that
        if the database supports an auto-commit feature, this must
        be initially off. An interface method may be provided to
        turn it back on.
        Database modules that do not support transactions should
        implement this method with void functionality.
        """
        for table in self.tables.keys():
            if self.tables[table].open:
                self.tables[table].commit()
        self.createdTables = []

    @_raise_closed
    def rollback(self):
        """
        In case a database does provide transactions this method
        causes the the database to roll back to the start of any
        pending transaction.  Closing a connection without
        committing the changes first will cause an implicit
        rollback to be performed.
        """
        for table in self.tables.keys():
            if self.tables[table].open:
                self.tables[table].rollback()
        for table in self.createdTables:
            if table in self.tables:
                if self.tables[table].open:
                    self.tables[table]._close()
                del self.tables[table]
            for end in self.tableExtensions:
                if os.path.exists(self.database + os.sep + table + end):
                    os.remove(self.database + os.sep + table + end)
        self.createdTables = []

    @_raise_closed
    def cursor(self) -> Cursor:
        """Return a new Cursor Object using the connection.  If the
        database does not provide a direct cursor concept, the
        module will have to emulate cursors using other means to
        the extent needed by this specification.  [4]"""
        return Cursor(self)

    # Type conversions
    @_raise_closed
    def _getConverters(self, table: str, columns):
        typeConverters = []
        sqlConverters = []
        try:
            table_ = self.tables[table]
        except KeyError:
            # if table not in self.tables:
            raise SQLError(f"Table '{table}' doesn't exist.")
        for column in columns:
            # if not self.tables[table].columnExists(column):
            #     raise SQLError(
            #         f"Table '{table}' has no column named '{column}'.")
            try:
                column_type = table_.get(column).type.capitalize()
                column_converter = self.driver['converters'][column_type]
                typeConverters.append(column_converter.valueToStorage)
                sqlConverters.append(column_converter.SQLToStorage)
            except KeyError:
                raise SQLError(
                    f"Table '{table}' has no column named '{column}'.")
        return sqlConverters, typeConverters

    # SQL helpers
    @_raise_closed
    def createDatabase(self):
        "Create the database"
        # [table, column, type, required, unique, primaryKey, default]

        if not self.databaseExists():
            # XXX Does this need to check files don't already exist?
            if not os.path.exists(self.database):
                os.mkdir(self.database)
            # if self.tables.has_key(self.colTypesName):
            if self.colTypesName in self.tables:
                raise Error("ColTypes table already exists.")
            self.tables[self.colTypesName] = self.driver['Table'](
                self.colTypesName, filename=self.database+os.sep +
                self.colTypesName, columns=[])
            self.tables[self.colTypesName]._load()
            types = ['String', 'String', 'String', 'Bool', 'Bool', 'Bool',
                     'Text', 'Text', 'Integer']
            self._insertRow(self.colTypesName, '1',
                            [self.colTypesName, 'TableName',  'String', 1, 0,
                             0, None, None, 0], types)
            self._insertRow(self.colTypesName, '2',
                            [self.colTypesName, 'ColumnName', 'String', 1, 0,
                             0, None, None, 1], types)
            self._insertRow(self.colTypesName, '3',
                            [self.colTypesName, 'ColumnType', 'String', 1, 0,
                             0, None, None, 2], types)
            self._insertRow(self.colTypesName, '4',
                            [self.colTypesName, 'Required',   'Bool',   0, 0,
                             0, None, None, 3], types)
            self._insertRow(self.colTypesName, '5',
                            [self.colTypesName, 'Unique',     'Bool',   0, 0,
                             0, None, None, 4], types)
            self._insertRow(self.colTypesName, '6',
                            [self.colTypesName, 'PrimaryKey', 'Bool',   0, 0,
                             0, None, None, 5], types)
            self._insertRow(self.colTypesName, '7',
                            [self.colTypesName, 'ForeignKey', 'Text',   0, 0,
                             0, None, None, 6], types)
            self._insertRow(self.colTypesName, '8',
                            [self.colTypesName, 'Default',    'Text',   0, 0,
                             0, None, None, 7], types)
            self._insertRow(self.colTypesName, '9',
                            [self.colTypesName, 'Position',   'Integer', 1, 0,
                             0, None, None, 8], types)
            self.tables[self.colTypesName].commit()
        else:
            raise DatabaseError("The database '%s' already exists."
                                % (self.database))

    @_raise_closed
    def _loadTableStructure(self):
        "Get the values from the ColTypes table into a suitable structure."
        # if not self.tables.has_key(self.colTypesName):
        if self.colTypesName not in self.tables:
            self.tables[self.colTypesName] = self.driver['Table'](
                self.colTypesName,
                filename=self.database + os.sep + self.colTypesName,
                columns=[])
            self.tables[self.colTypesName]._load()
        if not self.tables[self.colTypesName].open:
            raise Error("No Coltypes File loaded.")
        vals = []
        keys = self.tables[self.colTypesName].file.keys()
        for k in keys:
            row = self._getRow(self.colTypesName, k)
            vals.append([k, row])
        vals.sort()
        tables = {}
        for val in vals:        # Get info in the correct format
            v = val[1]
            if v[2] not in self.driver['converters'].keys():
                raise ConverterError(
                    "No converter registered for '%s' used in table '%s' "
                    "column '%s'." % (v[2], v[0], v[1]))
            # if not tables.has_key(v[0]):
            if v[0] not in tables:
                tables[v[0]] = []
            tables[v[0]].append(
                self.driver['Column'](
                    table=v[0],
                    name=v[1],
                    col_type=v[2],
                    required=v[3],
                    unique=v[4],
                    primaryKey=v[5],
                    foreignKey=v[6],
                    default=v[7],
                    converter=self.driver['converters'][v[2]],
                    position=v[8],
                )
            )
        self._checkTableFilesExist(tables.keys())
        for name, columns in tables.items():
            if name != self.colTypesName:
                self.tables[name] = self.driver['Table'](
                    name, filename=self.database+os.sep + name,
                    columns=columns)
            else:
                self.tables[name].columns = columns
        for name, columns in self.tables.items():
            for column in columns:
                if column.primaryKey:
                    self.tables[name].primaryKey = column.name
                if column.foreignKey:
                    self.tables[column.foreignKey].childTables.append(name)
                    self.tables[name].parentTables.append(column.foreignKey)

    @_raise_closed
    def _insertRowInColTypes(self, table):
        "Insert the data from Table Structure into ColTypes"
        primaryKey = int(self._getNewKey(self.colTypesName))
        counter = 0
        for col in self.tables[table].columns:
            self._insertRow(self.colTypesName, primaryKey+counter, [
                    col.table,
                    col.name,
                    col.type,
                    col.required,
                    col.unique,
                    col.primaryKey,
                    col.foreignKey,
                    col.default,
                    col.position
                ], types=['String', 'String', 'String', 'Bool', 'Bool',
                          'Bool', 'Text', 'Text', 'Integer']
            )
            counter += 1

    @_raise_closed
    def _checkTableFilesExist(self, tables):
        for table_ in tables:
            # Check each of the tables listed actually exists
            for end in self.tableExtensions:
                if not os.path.exists(self.database+os.sep + table_ + end):
                    raise CorruptionError("Table file '%s' not found." %
                                          (table_ + end))

    @_raise_closed
    def _getColumnPositions(self, table, columns):
        cols = []
        # if not self.tables.has_key(table):
        if table not in self.tables:
            raise InternalError("No such table '%s'." % (table))
        for column in columns:
            if not self.tables[table].columnExists(column):
                raise SQLError("'%s' is not a column in table '%s'." %
                               (column, table))
            cols.append(self.tables[table].get(column).position)
        return cols

    @_raise_closed
    def _convertValuesToInternal(self, table, columns, sqlValues=[],
                                 values=[]):
        # ~ if values and sqlValues:
        # ~ if len(sqlValues) <> len(values):
        # ~ raise SQLError("The number of ? doesn't match the number
        # of values.")
        sqlConverters, typeConverters = self._getConverters(table, columns)
        internalValues = []

        i = 0
        length = len(values)
        if len(sqlValues) == 0:
            for value in values:
                internalValues.append(typeConverters[i](value))
                i += 1
            return internalValues, len(values)
        else:
            counter = 0
            for value in sqlValues:
                if value == '?':
                    if len(sqlValues) > counter:
                        if counter == length:
                            raise SQLError('Too many ? specified in SQL')
                        internalValues.append(
                            typeConverters[i](values[counter]))
                        counter += 1
                    else:
                        raise SQLError(
                            "Not enough values supplied in execute() to "
                            "substitue each '?'.")
                else:
                    try:
                        internalValues.append(sqlConverters[i](value))
                    except ConversionError:
                        raise SQLSyntaxError('Incorrect quoting - ' +
                                             str(sys.exc_info()[1]))
                i += 1
            return internalValues, counter

    @_raise_closed
    def _convertWhereToInternal(self, table, where='', values=[]):
        if where:
            columns = []
            tables = []
            sqlValues = []
            typeConverters = []
            sqlConverters = []
            for block in where:
                # log.debug(block)
                if not isinstance(block, str):  # TODO: should test is list
                    if '.' not in block[0]:
                        if not table:
                            raise SQLError(
                                'No table specified for column %s in WHERE '
                                'clause' % repr(block[0]))
                        else:  # Use the default table
                            tables.append(table)
                            columns.append(block[0])
                    else:
                        table, column = block[0].split('.')
                        columns.append(column)
                        if table not in self.tables:
                            raise SQLError(
                                "Table %s specified in WHERE clause doesn't "
                                "exist" % table)
                        else:
                            tables.append(table)
                    sqlValues.append(block[2])
                    # if not self.tables.has_key(tables[-1]):
                    if tables[-1] not in self.tables:
                        raise SQLError(
                            "Table %s specified in WHERE clause doesn't exist"
                            % (repr(tables[-1])))
                    if not self.tables[tables[-1]].has_key(columns[-1]):
                        log.debug(where)
                        log.debug(columns)
                        raise SQLError(
                            "Table %s specified in WHERE clause doesn't have "
                            "a column %s" %
                            (repr(tables[-1]), repr(columns[-1])))
                    typeConverters.append(
                        self.driver['converters'][
                            self.tables[tables[-1]].get(columns[-1]).type
                            .capitalize()].valueToStorage)
                    sqlConverters.append(self.driver[
                        'converters'][self.tables[tables[-1]].get(columns[-1])
                                      .type.capitalize()].SQLToStorage)
            internalValues = []
            counter = 0
            i = 0
            length = len(values)
            for value in sqlValues:

                if value == '?':
                    if len(sqlValues) > counter:
                        if counter == length:
                            raise SQLError('Too many ? specified in SQL')
                        internalValues.append(
                            typeConverters[i](values[counter]))
                        counter += 1
                    else:
                        raise SQLError("Not enough values supplied in "
                                       "execute() to substitue each '?'.")
                else:
                    try:
                        internalValues.append(sqlConverters[i](value))
                    except ConversionError as e:
                        if isinstance(value, str) and value[0] != "'":
                            res1 = value.split('.')
                        if len(res1) == 1:
                            raise SQLError(str(e))
                        else:
                            t1, col1 = res1
                            internalValues.append([value])
                i += 1
            c = 0
            for i in range(len(where)):
                if not isinstance(where[i], str):  # TODO: should test is list
                    where[i][2] = internalValues[c]
                    c += 1
            return where, counter
        else:
            return [], 0

    # Database Internal Methods
    @_raise_closed
    def _tables(self):
        return self.tables.keys()

    def _columns(self, table):
        if self._closed:
            raise Error('The connection to the database has been closed.')

        if table not in self._tables():
            raise SQLError('The table %s does not exist' % (repr(table)))
        cols = []
        for i in range(len(self.tables[table].columns)):
            cols.append(None)
        for col in self.tables[table].columns:
            cols[col.position] = col.name
        return tuple(cols)

    # Actual SQL Methods
    @_raise_closed
    def _where(self, tables: Union[str, List[str]], where: list = []):
        "Where should contain None for NULLs"
        if isinstance(tables, str):
            tables = [tables]
        for table in tables:
            if table not in self.tables:
                raise InternalError("The table '%s' doesn't exist." % table)
            # if table not in self.tables:  # TODO: look redundent
            #     self.tables[table]._load()
        # 1. Convert name to number in the array
        if len(where):
            columns = {}
            for table in tables:
                columns[table] = {}
                for column in self.tables[table].columns:
                    columns[table][column.name] = column.position
            #    #~ for block in where:
                    # ~ columns[table] = {}
            # ~ for block in where:
                # ~ if type(block) <> type(''):
                    # ~ res = block[0].split('.')
                    # ~ if len(res) == 1:
                    # ~     t = tables[0]
                    # ~     col = block[0]
                    # ~ else:
                    # ~     t, col = res
                    # ~ if not columns.has_key(t):
                    # ~     columns[t] = {}
                    # ~ if not columns[t].has_key(block[0]):
                    # ~     columns[t][col] = self.tables[t].get(col).position
            # 2. Build the if statemnt to look like this
            ifStatement = """
tables = %s
tabs={}
for table in tables:
    tabs[table] = []
    for primaryKey in self.tables[table].file.keys():
        tabs[table].append([primaryKey, self._getRow(table, primaryKey)])

found = []
""" % str(tables)
            tableString = []
            tabDepth = 0
            for table in tables:
                tableString.append("%sfor %sRow in tabs['%s']:" %
                                   (tabDepth*'    ', table, table))
                tabDepth += 1
            ifStatement += '\n'.join(tableString)
            ifStatement += """
%sif""" % ((tabDepth)*'    ')
    # ie we need to know the keyPosition of each tables row and the position
    # of each field we want to select against
            for block in where:
                # Prepare the values
                if isinstance(block, str):
                    ifStatement += ' '+block+' '
                else:
                    res = block[0].split('.')
                    if len(res) == 1:
                        t = tables[0]
                        col = block[0]
                    else:
                        t, col = res
                    if not self.tables[t].columnExists(col):
                        raise SQLError("'%s' in the WHERE clause is not one "
                                       "of the column names of table %s" %
                                       (col, t))
                    columnName = col
                    table = t
                    if block[1].lower() == 'like':
                        if '%%' in block[2]:
                            raise SQLSyntaxError("You cannot have '%%' in a "
                                                 "LIKE clause. To escape a '%'"
                                                 " character use '\\%'.")
                        ifStatement += (" like(%sRow[1][columns['%s']['%s']], "
                                        "%s)" % (table, table, columnName,
                                                 repr(block[2])))
                    else:
                        if block[1] == '=':
                            logicalOperator = '=='
                        elif block[1] == '<>':
                            logicalOperator = '!='
                        else:
                            logicalOperator = block[1]
                        if block[2] is None:
                            value = None
                        else:
                            if isinstance(block[2], list):
                                res1 = block[2][0].split('.')
                                if len(res1) == 1:
                                    raise SQLSyntaxError(
                                        "No '.' character found in right "
                                        "operand column %s in WHERE clause" %
                                        (repr(block[2])))
                                else:
                                    t1, col1 = res1
                                    value = (" %sRow[1][columns['%s']['%s']]"
                                             % (t1, t1, col1))
                            else:
                                value = repr(block[2])
                        ifStatement += (" %sRow[1][columns['%s']['%s']] %s %s"
                                        % (table, table, columnName,
                                           logicalOperator, value))
            tablesJoined = []
            for table in tables:
                tablesJoined.append("%sRow[0]" % (table))
            ifStatement += (":\n%sfound.append((%s))\n" %
                            ((tabDepth+1)*'    ', ', '.join(tablesJoined)))
            # 3. Now execute the code for each value in the database to get a
            # list of keys
            try:
                # log.info(ifStatement)
                exec(ifStatement)
                # print('-----', locals())
                # print(locals()['found'])
            except Exception:
                raise Bug("Exception: " + str(sys.exc_info()[1]) + "If: "
                          + ifStatement + '\n\nWhere: ' + str(where))
        else:
            return self.tables[table].file.keys()
        return locals()['found']

    @_raise_closed
    def _getNewKey(self, table):
        if table not in self.tables:
            raise InternalError("There is no such table '%s'." % table)
        else:
            for col in self.tables[table].columns:
                if col.primaryKey:
                    raise SQLError("The table '%s' has a primary key. You "
                                   "cannot obtain a new integer key for it."
                                   % table)
            keys = self.tables[table].file.keys()
            if not keys:
                return 1
            else:
                keyints = []
                for key in keys:
                    try:
                        # keyints.append(long(key))
                        keyints.append(int(key))
                    except Exception:
                        raise Bug('Keys for tables without a PRIMARY KEY '
                                  'specified should be capable of being used '
                                  'as integers or longs, %s in not a valid '
                                  'key.' % (repr(key)))
                m = max(keyints)
                try:
                    m = int(m)  # long(m)
                except Exception:
                    raise SQLError(
                        "Invalid primary key '%s' for the '%s' table."
                        % (m, table))
                else:
                    return str(m + 1)

    @_raise_closed
    def _create(self, table, columns, values):
        columns = columns
        # Check the table doesn't already exist
        # Add to the list of created tables in case of a rollback
        self.createdTables.append(table)
        # if self.tables.has_key(table):
        if table in self.tables:
            raise SQLError("Table '%s' already exists." % table)
        # Add the column information to the ColTypes table and the
        # tableStructure
        if not self.tables[self.colTypesName].open:
            self.tables[self.colTypesName]._load()
        # Add to tableStructure
        cols = []
        counter = 0
        defaultsUsed = 0
        for column in columns:
            if (column['type'].capitalize() not in
                    self.driver['converters'].keys()):
                raise SQLError("The type '%s' selected for column '%s' isn't "
                               "supported." % (column['type'], column['name']))
            default = 'NULL'
            if column['default'] is not None:
                default = column['default']
            if default == '?':
                if len(values) == defaultsUsed:
                    raise SQLError("Not enough values specified for '?' "
                                   "parameter substitution")
                default = self.driver['converters'][column[
                    'type'].capitalize()].valueToStorage(values[defaultsUsed])
                defaultsUsed += 1
            else:
                default = self.driver['converters'][column[
                    'type'].capitalize()].SQLToStorage(default)
            if column['foreignKey']:
                try:
                    t = column['foreignKey']
                except ValueError:
                    raise SQLSyntaxError('Invalid value %s for FOREIGN KEY - '
                                         'should be of the form table.column'
                                         % (repr(column['foreignKey'])))
                if not self.tables.has_key(t):
                    raise SQLError('Table %s specified in FOREIGN KEY option '
                                   'does not exist' % (repr(t)))
                f = False
                for c in self.tables[t].columns:
                    if c.primaryKey:
                        f = c
                if f is False:
                    raise SQLError('Table %s specified in FOREIGN KEY option '
                                   'does not have a PRIMARY KEY' % (repr(t)))
                if column['type'].capitalize() != f.type:
                    raise SQLError('Column %s specified in FOREIGN KEY option '
                                   'is not of the same type %s as PRIMARY KEY '
                                   'in table %s'
                                   % (repr(f.name), repr(f.type), repr(t)))
            cols.append(
                self.driver['Column'](
                    table,
                    column['name'],
                    column['type'].capitalize(),
                    column['required'],
                    column['unique'],
                    column['primaryKey'],
                    column['foreignKey'],
                    default,
                    self.driver['converters'][column['type'].capitalize()],
                    counter,
                )
            )
            counter += 1
        self.tables[table] = self.driver[
            'Table'](table, filename=self.database+os.sep + table,
                     columns=cols)
        self.tables[table]._load()
        # Add to ColTypes table
        self._insertRowInColTypes(table)
        return {
            'affectedRows': 0,
            'columns': ['TableName', 'ColumnName', 'ColumnType', 'Required',
                        'Unique', 'PrimaryKey', 'ForeignKey', 'Default',
                        'Position'],
            'table': table,
            'results':  None,
        }

    @_raise_closed
    def _drop(self, tables):
        if isinstance(tables, str):
            tables = [tables]
        for table in tables:
            if table not in self.tables:
                raise SQLError("Cannot drop '%s'. Table not found." % (table))
            if not self.tables[table].open:
                self.tables[table]._load()
            # Check foreign key constraints:
            # cannot drop a parent table until all children are removed, can
            # drop child table
            if self.tables[table].childTables:
                for child in self.tables[table].childTables:
                    if child not in tables:
                        raise SQLForeignKeyError(
                            'Cannot drop table %s since child table %s has'
                            ' a foreign key reference to it'
                            % (repr(table), repr(child)))
        for table in tables:
            # Remove from ColTypes table
            self._delete(self.colTypesName,
                         where=[['TableName', "=", "'" + table + "'"]])
            # Close table and remove from files list
            if table in self.tables:
                self.tables[table]._close()
                del self.tables[table]
            # Delete the actual files
            self._deleteTableFromDisk(table)
            # Delete table structure
            if table in self.createdTables:
                self.createdTables.pop(self.createdTables.index(table))
        return {
            'affectedRows': 0,
            'columns': None,
            'table': tables,
            'results':  None,
        }

    @_raise_closed
    def _insert(self, table, columns, sqlValues=[], values=[]):
        if table not in self.tables:
            raise SQLError("Table '%s' not found." % (table))
        internalValues, used = self._convertValuesToInternal(table, columns,
                                                             sqlValues, values)
        if not self.tables[table].open:
            self.tables[table]._load()
        # Get a new primaryKey
        primaryKey = None
        for col in self.tables[table].columns:
            if col.primaryKey:
                primaryKey = col.name
                break
        if primaryKey and primaryKey not in columns:
            raise SQLKeyError("PRIMARY KEY '%s' must be specified when "
                              "inserting into the '%s' table."
                              % (primaryKey, table))
        elif not primaryKey:
            keyval = self._getNewKey(table)
        else:
            keyval = internalValues[columns.index(primaryKey)]
            if self.tables[table].file.has_key(keyval):
                raise SQLKeyError(
                    "Row with the PRIMARY KEY '%s' already exists." % (keyval))
        # XXX Should unique mean "unique apart from NULLs"?
        for col in self.tables[table].columns:
            name = col.name
            # Check other internalValues needing to be unique are
            if col.unique and name in columns:
                for primaryKey in self.tables[table].file.keys():
                    row = self._getRow(table, primaryKey)
                    oldval = row[col.position]
                    val = internalValues[columns.index(name)]
                    if val is not None and val == oldval:
                        raise SQLError("The UNIQUE column '%s' already has a "
                                       "value '%s'." % (name, val))
            if col.required and name not in columns:
                raise SQLError(
                    "The REQUIRED value '%s' has not been specified." % (name))
            if col.required and internalValues[columns.index(name)] is None:
                raise SQLError(
                    "The REQUIRED value '%s' cannot be NULL." % (name))
            if col.primaryKey:
                if name not in columns:
                    # XXX Already specified.
                    raise SQLError(
                        "The PRIMARY KEY '%s' has not been specified." %
                        (name))
                elif internalValues[columns.index(name)] is None:
                    raise SQLError(
                        "The PRIMARY KEY value '%s' cannot be NULL." % (name))

        # Arrange the internalValues in the correct order, filling defaults as
        # necessary
        cols = []
        for col in self.tables[table].columns:
            cols.append([col.position, col.name])
        cols.sort()
        columnNames = []
        defaults = []
        for col in cols:
            columnNames.append(col[1])
            defaults.append(self.tables[table].get(col[1]).default)
        vals = []
        for col in columns:
            if col not in columnNames:
                raise SQLError("Column '%s' does not exist in table '%s'."
                               % (col, table))
            defaults[columnNames.index(col)] = \
                internalValues[columns.index(col)]
            vals.append(internalValues[columns.index(col)])

        # Check foreign keys are specified if needed
        if self.tables[table].parentTables:
            for column in self.tables[table].columns:
                if column.foreignKey:
                    if column.name not in columns:
                        raise SQLForeignKeyError(
                            "Foreign key %s not specified when inserting into "
                            "table %s" % (repr(column.name), repr(table)))
                    else:
                        v = []
                        results = self._select([
                            self.tables[column.foreignKey].primaryKey],
                            column.foreignKey, [], [])['results']
                        for result in results:
                            v.append(result[0])
                        if defaults[column.position] not in v:
                            raise SQLForeignKeyError(
                                "Invalid value for foreign key %s since "
                                "table %s does not have a primary key value %s"
                                % (repr(column.name), repr(column.foreignKey),
                                   repr(defaults[column.position])))

        self._insertRow(table, keyval, defaults)
        return {
            'affectedRows': 1,
            'columns': columns,
            'table': table,
            'results':  None,
        }

    @_raise_closed
    def _update(self, table, columns, where=[], sqlValues=[], values=[]):
        # if not self.tables.has_key(table):
        if table not in self.tables:
            raise SQLError("Table '%s' not found." % (table))
        internalValues, used = self._convertValuesToInternal(
            table, columns, sqlValues, values)
        where, used2 = self._convertWhereToInternal(table, where,
                                                    values[used:])
        if not used+used2 == len(values):
            raise SQLError('There are %s ? in the SQL but %s values have been '
                           'specified to replace them.' %
                           (used+used2, len(values)))
        if not self.tables[table].open:
            self.tables[table]._load()
        positions = self._getColumnPositions(table, columns)
        keys = self._where(table, where)
        # print keys
        # keys.append(result[0])
        if not keys:
            return {
                'affectedRows': 0,
                'columns': columns,
                'table': table,
                'results':  None,
            }
        elif len(keys) > 1:
            # Check that there isn't a or unique column being updated with more
            # than one value
            for col in self.tables[table].columns:
                if col.unique and col.name in columns:
                    raise SQLError(
                        "The UNIQUE column '%s' cannot be updated setting %s "
                        "values to %s." %
                        (col.name, len(keys),
                         repr(internalValues[columns.index(col.name)])))
                if col.primaryKey and col.name in columns:
                    raise SQLError(
                        "The PRIMARY KEY column '%s' cannot be updated setting"
                        " %s values to %s." %
                        (col.name, len(keys),
                         repr(internalValues[columns.index(col.name)])))
        newkey = None
        for col in self.tables[table].columns:
            # Check other internalValues needing to be unique are
            if col.unique and col.name in columns:
                for primaryKey in self.tables[table].file.keys():
                    if primaryKey not in keys:
                        oldval = self._getRow(table, primaryKey)[col.position]
                        val = internalValues[columns.index(col.name)]
                        if val is not None and val == oldval:
                            raise SQLError(
                                "The UNIQUE column '%s' already has a value "
                                "'%s'." % (col.name, oldval))
            if (col.required and col.name in columns and
                    internalValues[columns.index(col.name)] is None):
                raise SQLError("The REQUIRED value '%s' cannot be NULL."
                               % (col.name))
            # ie must be just one update otherwise would have raised an error
            # earlier
            if col.primaryKey:
                if col.name in columns:
                    if internalValues[columns.index(col.name)] is None:
                        raise SQLError(
                            "The PRIMARY KEY value '%s' cannot be NULL."
                            % (col.name))
                    for primaryKey in self.tables[table].file.keys():
                        if (primaryKey not in keys and
                                primaryKey ==
                                internalValues[columns.index(col.name)]):
                            raise SQLError(
                                "A PRIMARY KEY '%s' has already been "
                                "specified." % (col.name))
                    newkey = internalValues[columns.index(col.name)]
        # Check foreign key constraints
        # 1. Find out if this is a child table
        if self.tables[table].parentTables:
            # 2. See if we are updating any Foreign Keys
            counter = 0
            for column in columns:
                column = self.tables[table].get(column)
                if column.foreignKey:
                    v = []
                    results = self._select(
                        [self.tables[column.foreignKey].primaryKey],
                        column.foreignKey, [], [])['results']
                    for result in results:
                        v.append(result[0])
                if not internalValues[counter] in v:
                    raise SQLForeignKeyError(
                        "Invalid value for foreign key %s since table %s does "
                        "not have a primary key value %s" %
                        (repr(column.name), repr(column.foreignKey),
                         repr(internalValues[counter])))
                counter += 1

        vals = []
        # print keys
        for primaryKey in keys:
            try:
                row = self._getRow(table, primaryKey)
                for pos in range(len(positions)):
                    row[positions[pos]] = internalValues[pos]
                self._updateRow(table, primaryKey, newkey, row)
                vals.append(row)
            except Exception as e:
                print(e)
                raise DatabaseError(
                    "Table %s has no row with the primary key %s." %
                    (repr(table), repr(primaryKey)))
            """
            if not self.tables[table].file.has_key(primaryKey):
                raise DatabaseError("Table %s has no row with the primary key "
                                    "%s."%(repr(table), repr(primaryKey)))
            else:
                row = self._getRow(table, primaryKey)
                for pos in range(len(positions)):
                    row[positions[pos]] = internalValues[pos]
                self._updateRow(table, primaryKey, newkey, row)
                vals.append(row)
            """
        return {
            'affectedRows': len(keys),
            'columns': columns,
            'table': table,
            'results':  None,
        }

    @_raise_closed
    def _select(self, columns, tables, where, order, values=[]):
        if isinstance(tables, str):
            tables = [tables]
        for table in tables:
            # if not self.tables.has_key(table):
            if table not in self.tables:
                raise SQLError("Table '%s' not found." % (table))
            if not self.tables[table].open:
                self.tables[table]._load()
        cols = []
        if columns == ['*']:
            fullList = []
            if len(tables) == 1:
                for column in self._columns(tables[0]):
                    fullList.append(column)
                    cols.append(self.tables[tables[0]].get(column).position)
            else:
                for table in tables:
                    offset = len(fullList)
                    for column in self._columns(table):
                        fullList.append(table+'.'+column)
                        cols.append(offset +
                                    self.tables[table].get(column).position)
            columns = fullList
        elif len(tables) == 1:
            for i in range(len(columns)):
                if columns[i][:len(tables[0])+1] == tables[0]+'.':
                    columns[i] = columns[i].split('.')[1]
                elif '.' in columns[i]:
                    raise SQLSyntaxError(
                        "Table in column name %s is not listed after the FROM "
                        "part of the SELECT statement %s" %
                        (repr(columns[i].split()[0]), repr(columns[i])))
                cols.append(self._getColumnPositions(tables[0],
                                                     [columns[i]])[0])
        else:
            lengths = [0]
            for table in tables:
                if len(lengths) == 0:
                    lengths.append(len(self._columns(table)))
                else:
                    lengths.append(lengths[-1]+len(self._columns(table)))
            for column in columns:
                if '.' not in column:
                    raise SQLSyntaxError(
                        "Expected table name followed by a '.' character "
                        "before column name %s " % column)
                else:
                    res = column.split('.')
                    if len(res) != 2:
                        raise SQLError("Invalid column name %s too many '.' "
                                       "characters." % column)
                    cols.append(lengths[tables.index(res[0])] +
                                self._getColumnPositions(res[0], [res[1]])[0])
        where, used = self._convertWhereToInternal(table, where, values)
        if not used == len(values):
            raise SQLError('There are %s ? in the SQL but %s values have been '
                           'specified to replace them.' % (used, len(values)))
        keys = self._where(tables, where)
        if keys:
            rows = []
            for results in keys:
                if not isinstance(results, tuple):
                    r = self._getRow(tables[0], results)
                else:
                    r = []
                    for i in range(len(results)):
                        for term in self._getRow(tables[i], results[i]):
                            r.append(term)
                rows.append(r)
            results = []
            for row in rows:
                result = []
                for col in cols:
                    result.append(row[col])
                results.append(result)
            if order:
                orderDesc = []
                orderCols = []
                for order in order:
                    orderCols.append(order[0])
                    orderDesc.append(order[1])
                orderPos = self._getColumnPositions(table, orderCols)

                class OrderCompare:
                    def __init__(self, orderPos, orderDesc):
                        self.orderPos = orderPos
                        self.orderDesc = orderDesc

                    def __call__(self, x, y):
                        for i in range(len(self.orderPos)):
                            if x[self.orderPos[i]] < y[self.orderPos[i]]:
                                if self.orderDesc[i] == 'asc':
                                    return -1
                                else:
                                    return 1
                            elif x[self.orderPos[i]] > y[self.orderPos[i]]:
                                if self.orderDesc[i] == 'asc':
                                    return 1
                                else:
                                    return -1
                        return 0

                results.sort(OrderCompare(orderPos, orderDesc))
            if len(tables) == 1:
                tables = tables[0]
            return {
                'affectedRows': len(results),
                'columns': columns,
                'table': tables,
                'results':  tuple(results),
            }
        else:
            if len(tables) == 1:
                tables = tables[0]
            return {
                'affectedRows': 0,
                'columns': columns,
                'table': tables,
                'results': [],
            }

    @_raise_closed
    def _delete(self, table, where=[], values=[]):
        if table not in self.tables:
            raise SQLError("Table '%s' not found." % (table))
        if not self.tables[table].open:
            self.tables[table]._load()
        where, used = self._convertWhereToInternal(table, where, values)
        if not used == len(values):
            raise SQLError('There are %s ? in the SQL but %s values have been '
                           'specified to replace them.' % (used, len(values)))
        keys = self._where(table, where)
        # Check foreign key constraints
        # 1. Find out if this is a parent table
        if self.tables[table].childTables:
            # 2. If they do get all the distinct primary key values of the
            # table, cahing the values
            parentTableKeyValues = []
            results = self._select([self.tables[table].primaryKey], table, [],
                                   [])['results']
            for result in results:
                if result[0] in parentTableKeyValues:
                    raise Bug('Duplicate key values %s in table %s' %
                              (repr(table), repr(result[0])))
                else:
                    parentTableKeyValues.append(result[0])
            # 3. For all the child table columns with this table as a foreign
            # key, check the cache to make sure
            #    the value doesn't exist.
            for childTable in self.tables[table].childTables:
                foreignKeyPosition = None
                for column in self.tables[childTable].columns:
                    if column.foreignKey:
                        foreignKeyPosition = column.position
                if foreignKeyPosition is None:
                    raise Bug('No foreign key found in child table.')
                else:
                    if not self.tables[childTable].open:
                        self.tables[childTable]._load()
                    for key in self.tables[childTable].file.keys():
                        row = self._getRow(childTable, key)
                        if row[foreignKeyPosition] in parentTableKeyValues:
                            raise SQLForeignKeyError(
                                "Table %s contains references to record with "
                                "PRIMARY KEY %s in %s" %
                                (repr(childTable), repr(key), repr(table)))
        # Delete the rows
        for primaryKey in keys:
            if not self.tables[table].open:
                self.tables[table]._load()
            try:
                self._deleteRow(table, primaryKey)
            except Exception as e:
                print(e)
                raise DatabaseError("Table %s has no row with the primary key "
                                    "%s." % (repr(table), repr(primaryKey)))
            """
            if not self.tables[table].file.has_key(primaryKey):
                raise DatabaseError("Table %s has no row with the primary key "
                                    "%s."%(repr(table), repr(primaryKey)))
            else:
                self._deleteRow(table, primaryKey)
            """
        return {
            'affectedRows': len(keys),
            'columns': None,
            'table': table,
            'results': None,
        }

    @_raise_closed
    def _showTables(self):
        tables = self._tables()
        results = []
        for table in tables:
            results.append([table])
        return {
            'affectedRows': 0,
            'columns': ['Tables'],
            'table': None,
            'results': results,
        }
