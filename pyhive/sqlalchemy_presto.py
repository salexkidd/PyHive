"""Integration between SQLAlchemy and Presto.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from distutils.version import StrictVersion
from pyhive import presto
from sqlalchemy import exc
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler, expression, type_api, elements
from sqlalchemy.util.langhelpers import public_factory
import re
import sqlalchemy

try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler


class JsonExtractScalar(expression.ColumnElement):
    __visit_name__ = 'json_extract_scalar'

    def __init__(self, column, json_field, type_):
        self.type = type_api.to_instance(type_)
        self.column_clause = elements.ColumnClause(column, type_=None)
        self.json_field_clause = elements._literal_as_binds(json_field, type_=None)
        self.typeclause = elements.TypeClause(self.type)


class PrestoCompiler(SQLCompiler):
    def __init__(self, *args, **kwargs):
        super(PrestoCompiler, self).__init__(*args, **kwargs)

    def visit_json_extract_scalar(self, json_extract, **kwargs):
        return "json_extract_scalar(to_json(%s), %s)" % \
            (json_extract.column_clause._compiler_dispatch(self, **kwargs),
             json_extract.json_field_clause._compiler_dispatch(self, **kwargs))


json_extract_scalar = public_factory(
    JsonExtractScalar, ".expression.json_extract_scalar"
)


class PrestoIdentifierPreparer(compiler.IdentifierPreparer):
    # https://github.com/facebook/presto/blob/master/presto-parser/src/main/antlr3/com/facebook/presto/sql/parser/Statement.g
    reserved_words = frozenset([
        'all',
        'alter',
        'and',
        'approximate',
        'array',
        'as',
        'asc',
        'at',
        'bernoulli',
        'between',
        'bigint',
        'boolean',
        'by',
        'case',
        'cast',
        'catalog',
        'catalogs',
        'char',
        'character',
        'coalesce',
        'columns',
        'confidence',
        'constraint',
        'create',
        'cross',
        'current',
        'current_date',
        'current_time',
        'current_timestamp',
        'date',
        'day',
        'dec',
        'decimal',
        'desc',
        'describe',
        'distinct',
        'distributed',
        'double',
        'drop',
        'else',
        'end',
        'escape',
        'except',
        'exists',
        'explain',
        'extract',
        'false',
        'first',
        'following',
        'for',
        'format',
        'from',
        'full',
        'functions',
        'graphviz',
        'group',
        'having',
        'hour',
        'if',
        'in',
        'inner',
        'insert',
        'int',
        'integer',
        'intersect',
        'interval',
        'into',
        'is',
        'join',
        'json',
        'json_extract_scalar',
        'last',
        'left',
        'like',
        'limit',
        'localtime',
        'localtimestamp',
        'logical',
        'minute',
        'month',
        'natural',
        'not',
        'null',
        'nullif',
        'nulls',
        'number',
        'numeric',
        'on',
        'or',
        'order',
        'outer',
        'over',
        'partition',
        'partitions',
        'poissonized',
        'preceding',
        'range',
        'recursive',
        'rename',
        'replace',
        'rescaled',
        'right',
        'row',
        'rows',
        'schema',
        'schemas',
        'second',
        'select',
        'show',
        'stratify',
        'substring',
        'system',
        'table',
        'tables',
        'tablesample',
        'text',
        'then',
        'time',
        'timestamp',
        'to',
        'true',
        'try_cast',
        'type',
        'unbounded',
        'union',
        'use',
        'using',
        'values',
        'varchar',
        'varying',
        'view',
        'when',
        'where',
        'with',
        'year',
        'zone',
    ])


try:
    from sqlalchemy.types import BigInteger
except ImportError:
    from sqlalchemy.databases.mysql import MSBigInteger as BigInteger
_type_map = {
    'bigint': BigInteger,
    'boolean': types.Boolean,
    'double': types.Float,
    'varchar': types.String,
}


class PrestoDialect(default.DefaultDialect):
    name = 'presto'
    driver = 'rest'
    preparer = PrestoIdentifierPreparer
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None

    @classmethod
    def dbapi(cls):
        return presto

    def create_connect_args(self, url):
        db_parts = url.database.split('/')
        kwargs = {
            'host': url.host,
            'port': url.port,
            'username': url.username,
        }
        kwargs.update(url.query)
        if len(db_parts) == 1:
            kwargs['catalog'] = db_parts[0]
        elif len(db_parts) == 2:
            kwargs['catalog'] = db_parts[0]
            kwargs['schema'] = db_parts[1]
        else:
            raise ValueError("Unexpected database format {}".format(url.database))
        return ([], kwargs)

    def _get_table_columns(self, connection, table_name, schema):
        full_table = self.identifier_preparer.quote_identifier(table_name)
        if schema:
            full_table = self.identifier_preparer.quote_identifier(schema) + '.' + full_table
        try:
            return connection.execute('SHOW COLUMNS FROM {}'.format(full_table))
        except presto.DatabaseError as e:
            # Normally SQLAlchemy should wrap this exception in sqlalchemy.exc.DatabaseError, which
            # it successfully does in the Hive version. The difference with Presto is that this
            # error is raised when fetching the cursor's description rather than the initial execute
            # call. SQLAlchemy doesn't handle this. Thus, we catch the unwrapped
            # presto.DatabaseError here.
            # Does the table exist?
            msg = e.message.get('message') if isinstance(e.message, dict) else None
            regex = r"^Table\ \'.*{}\'\ does\ not\ exist$".format(re.escape(table_name))
            if msg and re.match(regex, msg):
                raise exc.NoSuchTableError(table_name)
            else:
                raise

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, None)
        result = []
        for row in rows:
            try:
                coltype = _type_map[row.Type]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" % (row.Type, row.Column))
                coltype = types.NullType
            result.append({
                'name': row.Column,
                'type': coltype,
                'nullable': row.Null,
                'default': None,
            })
        return result

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Hive has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Hive has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, None)
        col_names = []
        for row in rows:
            if row['Partition Key']:
                col_names.append(row['Column'])
        if col_names:
            return [{'name': 'partition', 'column_names': col_names, 'unique': False}]
        else:
            return []

    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' FROM ' + self.identifier_preparer.quote_identifier(schema)
        return [row.tab_name for row in connection.execute(query)]

    def do_rollback(self, dbapi_connection):
        # No transactions for Presto
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True

if StrictVersion(sqlalchemy.__version__) < StrictVersion('0.6.0'):
    from pyhive import sqlalchemy_backports

    def reflecttable(self, connection, table, include_columns=None, exclude_columns=None):
        insp = sqlalchemy_backports.Inspector.from_engine(connection)
        return insp.reflecttable(table, include_columns, exclude_columns)
    PrestoDialect.reflecttable = reflecttable
