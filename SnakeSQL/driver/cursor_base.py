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
                   SQLSyntaxError, SQLForeignKeyError, SQLKeyError,
                   InterfaceError, DataError)
from ..external.tablePrint import table_print
import datetime
# import types
import sys
import os
import logging
from ..external import SQLParserTools
# import dtuple
log = logging.getLogger()


class Cursor:
    """
    These objects represent a database cursor, which is used to
    manage the context of a fetch operation. Cursors created from
    the same connection are not isolated, i.e., any changes
    done to the database by a cursor are immediately visible by the
    other cursors. Cursors created from different connections can
    or can not be isolated, depending on how the transaction support
    is implemented (see also the connection's rollback() and commit()
    methods.)

    My note:
    SQL Values should be specified with single quotes.

    self.info has the following specification:
    'columns'      - list of column names from the result set
    'table'        - table name of result set
    'results'      - tuple of results or None if no result set
    'affectedRows' - number of affected rows.
    """

    def __init__(self, connection, debug=False, format='tuple'):
        self.debug = debug
        self.format = format
        self.sql = []
        self._closed = False
        self.connection = connection
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        self.info = {'affectedRows': 0, 'columns': [], 'results': (),
                     'table': ''}
        # DB-API 2.0 attributes:
        self.arraysize = 1
        """This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to 1 meaning
        to fetch a single row at a time.

        Implementations must observe this value with respect to
        the fetchmany() method, but are free to interact with the
        database a single row at a time. It may also be used in
        the implementation of executemany()."""
        self.position = 0

    def __getattr__(self, name):
        # ~ converter = None # Use the converter methods as class methods
        # ~ if name[:2] == 'to':
        # ~     converter = 'SQLTo' + name[2:]
        # ~ elif name[:5] == 'SQLTo' or name[5:] == 'ToSQL':
        # ~     converter = name
        # ~ if converter:
        # ~     return self.connection._converter(converter)
        # ~ el
        if name == 'rowcount':
            """This read-only attribute specifies the number of rows that
            the last executeXXX() produced (for DQL statements like
            'select') or affected (for DML statements like 'update' or
            'insert').

            The attribute is -1 in case no executeXXX() has been
            performed on the cursor or the rowcount of the last
            operation is not determinable by the interface. [7]

            Note: Future versions of the DB API specification could
            redefine the latter case to have the object return None
            instead of -1."""
            return self.info['affectedRows']
        elif name == 'description':
            """This read-only attribute is a sequence of 7-item
            sequences.  Each of these sequences contains information
            describing one result column: (name, type_code,
            display_size, internal_size, precision, scale,
            null_ok). The first two items (name and type_code) are
            mandatory, the other five are optional and must be set to
            None if meaningfull values are not provided.

            This attribute will be None for operations that
            do not return rows or if the cursor has not had an
            operation invoked via the executeXXX() method yet.

            The type_code can be interpreted by comparing it to the
            Type Objects specified in the section below."""
            if self.info['results'] is None:
                return None
            else:
                l = []
                if (self.info['table'] in self.connection.tables.keys() and
                        self.info['columns']):
                    for column in self.info['columns']:
                        l.append(
                            (
                                column,
                                _type_codes[self.connection.tables[
                                    self.info['table']].get(column).type],
                                None,
                                None,
                                None,
                                None,
                                self.connection.tables[
                                    self.info['table']].get(column).required,
                            )
                        )
                    return tuple(l)
                else:
                    raise Bug("No table specified or no columns present.")
        else:
            raise AttributeError("Cursor instance has no attribute %s"
                                 % (repr(name)))

    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then
        execute it against all parameter sequences or mappings
        found in the sequence seq_of_parameters.

        Modules are free to implement this method using multiple
        calls to the execute() method or by using array operations
        to have the database process the sequence as a whole in
        one call.

        Use of this method for an operation which produces one or
        more result sets constitutes undefined behavior, and the
        implementation is permitted (but not required) to raise
        an exception when it detects that a result set has been
        created by an invocation of the operation.

        The same comments as for execute() also apply accordingly
        to this method.

        Return values are not defined."""
        if self._closed:
            raise Error('The cursor has been closed.')
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def execute(self, operation, parameters=[]):
        """Prepare and execute a database operation (query or
        command).  Parameters may be provided as sequence or
        mapping and will be bound to variables in the operation.
        Variables are specified in a database-specific notation
        (see the module's paramstyle attribute for details). [5]

        A reference to the operation will be retained by the
        cursor.  If the same operation object is passed in again,
        then the cursor can optimize its behavior.  This is most
        effective for algorithms where the same operation is used,
        but different parameters are bound to it (many times).

        For maximum efficiency when reusing an operation, it is
        best to use the setinputsizes() method to specify the
        parameter types and sizes ahead of time.  It is legal for
        a parameter to not match the predefined information; the
        implementation should compensate, possibly with a loss of
        efficiency.

        The parameters may also be specified as list of tuples to
        e.g. insert multiple rows in a single operation, but this
        kind of usage is depreciated: executemany() should be used
        instead."""
        if self._closed:
            raise Error('The cursor has been closed.')
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        # start web.database
        if self.debug:
            self.sql.append(sql)
        # end web.database
        if type(parameters) not in [type(()), type([])]:
            parameters = [parameters]
        parsedSQL = self.connection.parser.parse(operation)
        if parsedSQL['function'] == 'create':
            self.info = self.connection._create(
                parsedSQL['table'], parsedSQL['columns'], parameters)
        elif parsedSQL['function'] == 'drop':
            self.info = self.connection._drop(parsedSQL['tables'])
        elif parsedSQL['function'] == 'insert':
            self.info = self.connection._insert(
                parsedSQL['table'], parsedSQL['columns'],
                parsedSQL['sqlValues'], parameters)
        elif parsedSQL['function'] == 'update':
            self.info = self.connection._update(
                table=parsedSQL['table'], columns=parsedSQL['columns'],
                where=parsedSQL.get('where', []),
                sqlValues=parsedSQL['sqlValues'], values=parameters)
            """
            if parsedSQL.has_key('where'):
                self.info = self.connection._update(
                    parsedSQL['table'], parsedSQL['columns'],
                    parsedSQL['where'], parsedSQL['sqlValues'], parameters)
            else:
                self.info = self.connection._update(
                    parsedSQL['table'], parsedSQL['columns'],
                    sqlValues = parsedSQL['sqlValues'], values = parameters)
            """
        elif parsedSQL['function'] == 'select':
            del parsedSQL['function']
            return self.select(**parsedSQL)
        elif parsedSQL['function'] == 'delete':
            self.info = self.connection._delete(
                parsedSQL['table'], where=parsedSQL.get('where', []),
                values=parameters)
            """
            if parsedSQL.has_key('where'):
                self.info = self.connection._delete(
                    parsedSQL['table'], parsedSQL['where'], parameters)
            else:
                self.info = self.connection._delete(
                    parsedSQL['table'], values=parameters)
            """
        elif parsedSQL['function'] == 'show':
            self.info = self.connection._showTables()
        else:
            raise SQLError("%s is not a supported keyword."
                           % parsedSQL['function'].upper())
        self.position = 0

    def fetchall(self, autoConvert=True, format=None):
        """Fetch all (remaining) rows of a query result, returning
        them as a sequence of sequences (e.g. a list of tuples).
        Note that the cursor's arraysize attribute can affect the
        performance of this operation.

        An Error (or subclass) exception is raised if the previous
        call to executeXXX() did not produce any result set or no
        call was issued yet."""
        if self._closed:
            raise Error('The cursor has been closed.')
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        return self.fetchmany('all', autoConvert, format)

    def fetchone(self, autoConvert=True, format=None):
        """Fetch the next row of a query result set, returning a
        single sequence, or None when no more data is
        available. [6]

        An Error (or subclass) exception is raised if the previous
        call to executeXXX() did not produce any result set or no
        call was issued yet."""
        if self._closed:
            raise Error('The cursor has been closed.')
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        if self.info['results'] is None:
            raise Error('Previous call to execute() did not produce a result '
                        'set. No results to fetch.')
        else:
            res = self.fetchmany(1, format)
            if res == ():
                return None
            else:
                return res[0]

    def fetchmany(self, size=None, autoConvert=True, format=None):
        """Fetch the next set of rows of a query result, returning a
        sequence of sequences (e.g. a list of tuples). An empty
        sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the
        parameter.  If it is not given, the cursor's arraysize
        determines the number of rows to be fetched. The method
        should try to fetch as many rows as indicated by the size
        parameter. If this is not possible due to the specified
        number of rows not being available, fewer rows may be
        returned.

        An Error (or subclass) exception is raised if the previous
        call to executeXXX() did not produce any result set or no
        call was issued yet.

        Note there are performance considerations involved with
        the size parameter.  For optimal performance, it is
        usually best to use the arraysize attribute.  If the size
        parameter is used, then it is best for it to retain the
        same value from one fetchmany() call to the next."""
        if self._closed:
            raise Error('The cursor has been closed.')
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        if self.info['results'] is None:
            raise Error('Previous call to execute() did not produce a result '
                        'set. No results to fetch.')
        else:
            results = None
            if autoConvert and self.info['table']:
                converters = []
                for column in self.info['columns']:
                    if isinstance(self.info['table'], list):
                        converters.append(
                            self.connection.tables[
                                column.split('.')[0]].get(column.split('.')[1])
                            .converter.storageToValue)
                    else:
                        converters.append(
                            self.connection.tables[
                                self.info['table']].get(column)
                            .converter.storageToValue)

                # print len(self.info['columns']), self.info['results'][0]
                rows = []
                for result in self.info['results']:
                    row = []

                    for i in range(len(result)):
                        c = converters[i]
                        r = result[i]
                        row.append(c(r))
                    rows.append(tuple(row))
                results = tuple(rows)
            else:
                results = tuple(self.info['results'])
            if size is None:
                # XXX returnVal = results[self.position:self.arraysize]
                #   self.arraysize ignored considered infinity.
                results = results[self.position:]
            elif size == 'all':
                pass  # results = results
            else:
                # results[self.position:self.position+size]
                # if max value returned
                # print len(results)
                if self.position + size <= len(results):
                    res = results[self.position:self.position+size]
                    self.position += size
                    results = res
                else:
                    return ()

            # start web.database
            if format is None:
                format = self.format
            if format == 'text':
                if results is not None:
                    return table(self.info['columns'], results, mode='sql')
            else:
                rows = []
                for row in results:
                    if format == 'dict':
                        dict = {}
                        for i in range(len(row)):
                            dict[self.info['columns'][i]] = row[i]
                        rows.append(dict)
                    elif format == 'object':
                        descr = dtuple.TupleDescriptor(
                            [[n] for n in self.info['columns']])
                        rows.append(
                            dtuple.DatabaseTuple(descr, row))
                    elif format == 'tuple':
                        rows.append(tuple(row))
                    else:
                        raise Bug("'%s' is not a valid option for format."
                                  % format)
                return tuple(rows)
            # end web.database

    # Unused DB-API 2.0 Methods
    def setinputsizes(self, sizes):
        """This can be used before a call to executeXXX() to
        predefine memory areas for the operation's parameters.

        sizes is specified as a sequence -- one item for each
        input parameter.  The item should be a Type Object that
        corresponds to the input that will be used, or it should
        be an integer specifying the maximum length of a string
        parameter.  If the item is None, then no predefined memory
        area will be reserved for that column (this is useful to
        avoid predefined areas for large inputs).

        This method would be used before the executeXXX() method
        is invoked.

        Implementations are free to have this method do nothing
        and users are free to not use it."""
        if self._closed:
            raise Error('The cursor has been closed.')
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        else:
            pass

    def setoutputsize(self, size, column=None):
        """Set a column buffer size for fetches of large columns
        (e.g. LONGs, BLOBs, etc.).  The column is specified as an
        index into the result sequence.  Not specifying the column
        will set the default size for all large columns in the
        cursor.

        This method would be used before the executeXXX() method
        is invoked.

        Implementations are free to have this method do nothing
        and users are free to not use it."""
        if self._closed:
            raise Error('The cursor has been closed.')
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        else:
            pass

    def close(self):
        """Close the cursor now (rather than whenever __del__ is
        called).  The cursor will be unusable from this point
        forward; an Error (or subclass) exception will be raised
        if any operation is attempted with the cursor."""
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        if self._closed:
            raise Error('The cursor has already been closed.')
        else:
            self._closed = True

    # Unimplemented DB-API 2.0 Methods
    # ~ def callproc(self, procname[,parameters])
    # ~     """(This method is optional since not all databases provide
    # ~     stored procedures. [3])
    # ~     Call a stored database procedure with the given name. The
    # ~     sequence of parameters must contain one entry for each
    # ~     argument that the procedure expects. The result of the
    # ~     call is returned as modified copy of the input
    # ~     sequence. Input parameters are left untouched, output and
    # ~     input/output parameters replaced with possibly new values.

    # ~     The procedure may also provide a result set as
    # ~     output. This must then be made available through the
    # ~     standard fetchXXX() methods."""
    # ~     pass

    # ~ def nextset(self)
    # ~     """(This method is optional since not all databases support
    # ~     multiple result sets. [3])

    # ~     This method will make the cursor skip to the next
    # ~     available set, discarding any remaining rows from the
    # ~     current set.

    # ~     If there are no more sets, the method returns
    # ~     None. Otherwise, it returns a true value and subsequent
    # ~     calls to the fetch methods will return rows from the next
    # ~     result set.

    # ~     An Error (or subclass) exception is raised if the previous
    # ~     call to executeXXX() did not produce any result set or no
    # ~     call was issued yet."""
    # ~     pass

    # Non DB-API Methods
    def __del__(self):
        if not self.connection._closed and not self._closed:
            return self.close()

    def tables(self):
        if self._closed:
            raise Error('The cursor has been closed.')
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        return self.connection._tables()

    def columns(self, table):
        if self._closed:
            raise Error('The cursor has been closed.')
        if self.connection._closed:
            raise Error('The connection to the database has been closed.')
        return self.connection._columns(table)

    # web.database API
    def tableExists(self, table):
        if table in self.tables():
            return True
        else:
            return False

    def columnExists(self, column, table):
        if column in self.columns(table):
            return True
        else:
            return False

    # def execute():
    #   if self.debug:
    #       self.sql.append(sql)

    # def fetchall(format=None):
    #
#
# SQL statement generators
#

    def select(self, columns, tables, where=None, order=None, execute=None,
               format=None, distinct=False):
        # if as <> None:
        #    raise NotSupportedError("SnakeSQL doesn't support aliases.")
        # if distinct:
        #    raise NotSupportedError("SnakeSQL doesn't support the DISTINCT "
        #                            "keyword.")
        if format is None:
            format = self.format
        if isinstance(columns, str):
            columns = [columns]
        if isinstance(tables, str):
            tables = [tables]
        if execute is False:
            return self.connection.parser.buildSelect(tables, columns, where,
                                                      order)
        else:
            # Don't need to worry about convertResult since it is taken care
            #   of in fetchRows()
            # Don't need to worry about format since it is taken care of in
            #   fetchRows()
            if isinstance(where, str):
                where = self.where(where)
            if isinstance(order, str):
                order = self.order(order)
            # if columns == ['*']:
            #    columns = self.columns(table)
            self.info = self.connection._select(
                columns=columns,
                tables=tables,
                where=where,
                order=order,
            )
            return self.fetchall(format=format)

    def insert(self, table, columns, values=None, sqlValues=None,
               execute=None):
        if sqlValues is None and values is None:
            raise SQLError('You must specify either values or sqlValues, they '
                           'can be []')
        if sqlValues is not None and values is not None:
            raise SQLError('You cannot specify both values and sqlvalues')
        if isinstance(columns, str):
            columns = [columns]
        if not isinstance(values, (tuple, list)):
            values = [values]
        if type(sqlValues) not in [type((1,)), type([])]:
            sqlValues = [sqlValues]
        if ((sqlValues != [None] and len(columns) != len(sqlValues)) or
                (values != [None] and len(columns) != len(values))):
            log.debug(f'v:{len(values)} c:{len(columns)} s:{sqlValues}')
            raise SQLError('The number of columns does not match the number '
                           'of values')
        if execute is False:
            if sqlValues == [None]:
                sqlValues = []
                if table not in self.connection.tables:
                    raise SQLError("Table '%s' doesn't exist." % table)
                for i in range(len(columns)):
                    if not self.connection.tables[table].columnExists(
                            columns[i]):
                        raise SQLError("Table '%s' has no column named '%s'."
                                       % (table, columns[i]))
                    sqlValues.append(
                        self.connection.driver['converters'][
                            self.connection.tables[table].get(columns[i])
                            .type.capitalize()].valueToSQL(values[i]))
            return self.connection.parser.buildInsert(table, columns,
                                                      sqlValues)
        else:
            self.info = self.connection._insert(
                table=table,
                columns=columns,
                values=values,  # Note: not sqlValues
            )
            # return sql

    def update(self, table, columns, values=None, sqlValues=None, where=None,
               execute=None):
        if sqlValues is None and values is None:
            raise SQLError('You must specify either values or sqlValues, they '
                           'can be []')
        if sqlValues is not None and values is not None:
            raise SQLError('You cannot specify both values and sqlvalues')
        if isinstance(columns, str):
            columns = [columns]
        if not isinstance(values, (tuple, list)):
            values = [values]
        if type(sqlValues) not in [type((1,)), type([])]:
            sqlValues = [sqlValues]
        if ((sqlValues != [None] and len(columns) != len(sqlValues)) or
                (values != [None] and len(columns) != len(values))):
            raise SQLError('The number of columns does not match the number '
                           'of values')
        if execute is False:
            if sqlValues == [None]:
                sqlValues = []
                if table not in self.connection.tables:
                    raise SQLError("Table '%s' doesn't exist." % table)
                for i in range(len(columns)):
                    if not self.connection.tables[table].columnExists(
                            columns[i]):
                        raise SQLError("Table '%s' has no column named '%s'."
                                       % (table, columns[i]))
                    sqlValues.append(self.connection.driver[
                        'converters'][self.connection.tables[table].get(
                            columns[i]).type.capitalize()].valueToSQL(
                                values[i]))
            return self.connection.parser.buildUpdate(table, columns,
                                                      sqlValues, where)
        else:
            if isinstance(where, str):
                where = self.where(where)
            self.info = self.connection._update(
                table=table,
                columns=columns,
                values=values,  # Note: not sqlValues
                where=where,
            )
            # return sql

    def delete(self, table, where=None, execute=None):
        if execute is False:
            return self.connection.parser.buildDelete(table, where)
        else:
            if isinstance(where, str):
                where = self.where(where)
            self.info = self.connection._delete(
                table=table,
                where=where,
            )
            # return sql

    def create(self, table, columns, execute=None):
        f = []
        for column in columns:
            if isinstance(column, str):
                f.append(self.column(column))
            else:
                f.append(column)
        if execute is False:
            return self.connection.parser.buildCreate(table, f)
        else:
            self.info = self.connection._create(
                table=table,
                columns=f,
            )
            # return sql

    def drop(self, tables, execute=None):
        "Remove a table from the database."
        if execute is False:
            return self.connection.parser.buildDrop(tables)
        else:
            self.info = self.connection._drop(tables=tables)
            # return sql

    # ~ def max(self, column, table, where=None):
    # ~     return self._function('max',column, table, where, True)

    # ~ def min(self, column, table, where=None):
    # ~     return self._function('min',column, table, where, True)

    # ~     def _function(self, func, column, table, where=None, execute=None):
    # ~         autoConvert = self._autoConvert
    # ~         if execute <> None:
    # ~             if execute not in self._autoExecuteOptions:
    # ~                 raise DatabaseError("execute must be one of %s."
    # ~                                     % (str(self._autoExecuteOptions),))
    # ~         else:
    # ~             execute = self._autoExecute

    # ~         if func.upper() in ['MAX', 'MIN']:
    # ~             if self._autoConvert == True:
    # ~                 if not self._typesCache.has_key(table.upper()):
    # ~                     self._getTableData(table)
    # ~             if not self._typesCache[table.upper()].has_key(
    # ~                     column.upper()):
    # ~                 raise Exception("No types information available for "
    # ~                           "'%s' column in table '%s'."%(column, table))
    # ~             self._colTypes = [self._typesCache[table.upper()][
    # ~                                 column.upper()]]
    # ~             self._colName = [column]

    # ~             sql = "SELECT %s(%s) FROM %s" % (func, column, table)
    # ~     if where:
    # ~         sql += " WHERE %s" % where

    # ~         self.execute(sql)
    # ~         val = self.fetchRows('tuple', None)[0][0]
    # ~         return val
    # ~     else:
    # ~         raise DatabaseError("The function '%s' is not supported by the"
    # ~                             " database module."%func)

#
# Builders
#
    def where(self, where):
        return self.connection.parser._parseWhere(where)

    def order(self, order):
        return self.connection.parser._parseOrder(order)

    def column(self, **params):
        values = {
            'name': None,
            'type': None,
            'required': 0,
            'unique': 0,
            'primaryKey': 0,
            'foreignKey': None,
            'default': None,
        }
        for key in params.keys():
            values[key.lower()] = params[key]
        if values['name'] is None:
            raise InterfaceError(
                "Parmeter 'name' not specified correctly for the column")
        if values['type'] is None:
            raise InterfaceError(
                "Parmeter 'name' not specified correctly for the column")
        if values['primaryKey'] and values['default'] is not None:
            raise InterfaceError(
                "A PRIMARY KEY column cannot also have a default value")
        if values['primaryKey'] and values['foreignKey'] is not None:
            raise InterfaceError(
                "A PRIMARY KEY column cannot be a FOREIGN KEY value")
        if values['default'] and values['foreignKey'] is not None:
            raise InterfaceError(
                "A FOREIGN KEY column cannot also have a default value")
        for i in ['required', 'unique', 'primaryKey']:
            if values[i] not in [0, 1, True, False]:
                raise DataError(
                    "%s can be True or False, not %s" % ([i], repr(values[i])))
        if (values['type'].capitalize() not in
            ['Date', 'Datetime', 'Time', 'Float', 'Long', 'String', 'Bool',
             'Text', 'Integer']):
            raise DataError(
                "%s is not a recognised type"
                % (values['type'].capitalize()))
        # XXX Type checking of default done later.
        # XXX Extra fields error
        # XXX Check foreign key details.
        return values
