from sqlalchemy import types
from ctypes import util
import re
import logging

from numba.core.typing.cffi_utils import _type_map
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.dialects import registry
from sqlalchemy.sql.compiler import IdentifierPreparer

# from wherobots.db import connect as wherobots_connect
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime
from wherobots.db.errors import OperationalError
from sqlalchemy.exc import NoSuchTableError
import logging

print("\n\n\nRunning within wherobots-python-dbapi\n\n\n")

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

_type_map = {
    'boolean': types.Boolean,
    'smallint': types.SmallInteger,
    'int': types.Integer,
    'bigint': types.BigInteger,
    'float': types.Float,
    'double': types.Float,
    'string': types.String,
    'varchar': types.String,
    'char': types.String,
    'binary': types.String,
    'array': types.String,
    'map': types.String,
    'struct': types.String,
    'geometry': types.String
}

class WherobotsDialect(DefaultDialect):
    name = 'Wherobots'
    driver = 'Wherobots'
    paramstyle = 'named'
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_boolean = True
    supports_statement_cache = True
    supports_dynamic_schema = True
    supports_catalog = True

    @classmethod
    def dbapi(cls):
        logger.info(f"dbapi() - running...")
        import wherobots.db as wherobots_db
        wherobots_db.paramstyle = 'named'
        return wherobots_db

    def create_connect_args(self, url):
        logger.info("create_connect_args() - running...")
        runtime = url.query.get("runtime", "SEDONA")
        region = url.query.get("region", "AWS_US_WEST_2")
        ws_url = url.query.get("ws_url")
        catalog = url.query.get("catalog")
        schema = url.query.get("schema")

        if isinstance(runtime, str):
            runtime = Runtime[runtime]

        if isinstance(region, str):
            region = Region[region]

        opts = {
            "host": url.host,
            "api_key": url.username,
            "runtime": runtime,
            "region": region,
            "ws_url": ws_url,
            "catalog": catalog,
            "schema": schema
        }

        return ([], opts)

    def get_schema_names(self, connection, **kw):
        logger.info(f"get_schema_names() - Fetching schema names...")
        try:
            catalog = connection.info.get('catalog', 'wherobots_open_data')
            query = f"SHOW SCHEMAS IN {catalog}" if catalog else "SHOW SCHEMAS"
            logger.info(f"get_schema_names() - query - {query}")
            result = connection.execute(query)
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"get_schema_names() - Error fetching schema names: {e}")
            raise

    def _get_table_columns(self, connection, table_name, schema):
        logger.info(f"_get_table_columns() - Fetching columns for table {table_name} in schema {schema}...")
        catalog = connection.info.get('catalog', 'wherobots_open_data')
        full_table = table_name
        if schema:
            full_table = f"{catalog}.{schema}.{table_name}" if catalog else f"{schema}.{table_name}"
        try:
            rows = connection.execute(f'DESCRIBE {full_table}').fetchall()
        except OperationalError as e:
            regex_fmt = r'TExecuteStatementResp.*SemanticException.*Table not found {}'
            regex = regex_fmt.format(re.escape(full_table))
            if re.search(regex, str(e)):
                raise NoSuchTableError(full_table)
            else:
                raise
        else:
            regex = r'Table .* does not exist'
            if len(rows) == 1 and re.match(regex, rows[0].col_name):
                raise NoSuchTableError(full_table)
            return rows

    def has_table(self, connection, table_name, schema=None, **kw):
        logger.info(f"has_table() - running...")
        logger.info(f"Checking existence of table {table_name} in schema {schema}...")
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except NoSuchTableError:
            return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        logger.info(f"get_columns() - running...")
        logger.info(f"get_columns() - Fetching columns for table {table_name} in schema {schema}...")
        rows = self._get_table_columns(connection, table_name, schema)
        rows = [[col.strip() if col else None for col in row] for row in rows]
        rows = [row for row in rows if row[0] and row[0] != '# col_name']
        result = []
        for (col_name, col_type, _comment) in rows:
            if col_name == '# Partition Information':
                break
            col_type = re.search(r'^\w+', col_type).group(0)

            try:
                coltype = _type_map[col_type]
            except KeyError:
                logger.warning(f"Did not recognize type '{col_type}' of column '{col_name}'")
                coltype = types.NullType

            result.append({
                'name': col_name,
                'type': coltype,
                'nullable': True,
                'default': None,
            })
        return result

    def get_table_names(self, connection, schema=None, **kw):
        logger.info(f"get_table_names() - running...")
        catalog = connection.info.get('catalog', 'wherobots_open_data')
        query = 'SHOW TABLES'
        if schema:
            query += f' IN {catalog}.{schema}' if catalog else f' IN {schema}'
        try:
            logger.info(f"get_table_names() - query - {query}")
            result = connection.execute(query)
            return [row[1] for row in result] if result else []
        except Exception as e:
            logger.error(f"get_table_names() - Error fetching table names: {e}")
            raise

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        logger.info(f"get_pk_constraint() - running...")
        return {
            'constrained_columns': []
        }

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        logger.info(f"get_foreign_keys() - running...")
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        logger.info(f"get_indexes() - running...")
        return []

    def get_view_names(self, connection, schema=None, **kw):
        logger.info(f"get_view_names() - running...")
        return []

    def do_rollback(self, dbapi_connection):
        logger.info(f"do_rollback() - running...")
        pass

    def do_rollback_to_savepoint(self, connection, name):
        logger.info(f"do_rollback_to_savepoint() - running...")
        pass

    def do_commit(self, dbapi_connection):
        logger.info(f"do_commit() - running...")
        pass

    def do_execute(self, cursor, statement, parameters, context=None):
        logger.info(f"do_execute() - running...")
        try:
            logger.info(f"do_execute() - Trying {statement} with {parameters}")
            cursor.execute(statement, parameters)
            cursor.fetchall()
        except Exception as e:
            logger.info(f"do_execute() - Error executing statement - {e}")
            raise
