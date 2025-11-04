import csv
import logging
import logging.handlers
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Iterable, Optional, Literal, Tuple

import oracledb
from oracledb import DatabaseError

from configuration import SQLLoaderOptions, DefaultFormatOptions
from db_common.db_connection import DbConnection
from db_writer.sql_loader import SQLLoaderExecutor
from db_writer.table_schema import TableSchema, ColumnSchema


class OracleConnection(DbConnection):

    def __init__(self, username: str, password: str, host: str, port: int, service_name: str,
                 insta_client_path: str = os.environ.get("HOME") + "/Downloads/instantclient_19_8",
                 logger: str = __name__):
        self.__username = username
        self.__password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self._insta_client_path = insta_client_path
        self.__connection: oracledb.Connection | None = None
        self._connected = False
        self._logger = logging.getLogger(logger)

    @property
    def dsn(self) -> str:
        return f'{self.host}:{self.port}/{self.service_name}'

    @property
    def connected(self) -> bool:
        return self._connected

    @staticmethod
    def escape(identifier: str) -> str:
        return f'"{identifier.upper()}"'

    def test_connection(self) -> None:
        self.connect()
        self.__connection.close()

    def connect(self) -> None:
        # You must always call init_oracle_client() to use thick mode in any platform
        oracledb.init_oracle_client(lib_dir=self._insta_client_path)

        self.__connection = oracledb.connect(user=self.__username, password=self.__password, dsn=self.dsn)
        self._connected = True

    @property
    def connection(self) -> oracledb.Connection:
        if not self.connected:
            raise WriterUserException("The connection is not initialized, please call connect() method first.")

        return self.__connection

    def run_procedure(self, procedure_name, parameters: list = None, keyword_parameters: dict = None):
        cur = self.connection.cursor()
        try:
            cur.callproc(procedure_name, parameters, keyword_parameters=keyword_parameters)
        except oracledb.DatabaseError as e:
            error, = e.args
            raise WriterUserException(f"Procedure '{procedure_name}', with parameters: {parameters} "
                                      f"call failed with error: {error.message}",
                                      db_error=error)
        finally:
            cur.close()

    def perform_query(self, query: str, bind_parameters: Optional[dict] = None) -> Iterable[dict]:
        """

        Args:
            query: Query string. Bind parameters are in query string prefixed with :. E.g. select * from t where ID=:id.
            bind_parameters: Dictionary of key value parameters to be bind to query. e.g. {"id":123}

        Returns:

            """
        cursor = self.connection.cursor()

        self._logger.debug(f'Running query: \n "{query}" \n '
                           f'Parameters: {bind_parameters}')
        try:
            cursor.execute(query, bind_parameters)
        except oracledb.DatabaseError as e:
            error, = e.args
            raise WriterUserException(f"Query failed with error: {error.message}",
                                      {"query": query, "parameters": bind_parameters}, db_error=error)

        try:
            for res in cursor.fetchall():
                yield res
        except oracledb.InterfaceError as e:
            if e.args[0].full_code == 'DPY-1003':
                self._logger.debug("Query returned no results..")

        cursor.close()

    def get_session_id(self) -> Tuple[int, int]:
        query = "SELECT SID, SERIAL# FROM V$SESSION WHERE AUDSID = Sys_Context('USERENV', 'SESSIONID')"
        res = self.perform_query(query)
        result = list(res)[0]
        return result[0], result[1]


class TableNotFoundError(Exception):
    pass


class OracleMetadataProvider:
    def __init__(self, connection: DbConnection):
        self.__connection = connection

    NO_LENGTH_DATATYPES = ('DATE', 'LONG', 'TIMESTAMP')
    NO_PRECISION_DATATYPES = ('FLOAT',)

    def get_table_metadata(self, schema: str, table_name: str) -> TableSchema:
        table_schema = TableSchema(table_name, [])
        query = """SELECT COLUMN_NAME, DATA_TYPE, 
                         DATA_LENGTH, DATA_PRECISION, NULLABLE as nullable  
                    FROM ALL_TAB_COLS 
                    where TABLE_NAME = :table_name AND OWNER = :schema"""  # noqa
        schema_norm = schema.strip().upper() if schema is not None else schema
        table_norm = table_name.strip().upper()
        rows = list(self.__connection.perform_query(query, {"table_name": table_norm, "schema": schema_norm}))
        if not rows:
            raise TableNotFoundError(f"The specified table {schema_norm}.{table_norm} was not found.")

        for res in rows:
            col = ColumnSchema(name=res[0],
                               source_type_signature=self._get_column_datatype_signature(res[1], res[2], res[3],
                                                                                         res[4]),
                               source_type=res[1],
                               length=res[2],
                               nullable=res[4])
            table_schema.add_column(col)
        return table_schema

    @staticmethod
    def _get_column_datatype_signature(dtype, length, precision, nullable) -> str:
        datatype = dtype
        dtype_stripped = dtype.split('(')[0]
        if length and dtype_stripped.upper() not in OracleMetadataProvider.NO_LENGTH_DATATYPES:
            datatype += f'({length}'
            if precision and dtype_stripped.upper() not in OracleMetadataProvider.NO_PRECISION_DATATYPES:
                datatype += f',{precision}'

            datatype += ')'

        datatype += ' NULL' if nullable else ' NOT NULL'

        return datatype


@dataclass
class OracleCredentials:
    username: str
    password: str
    host: str
    port: int
    service_name: str
    insta_client_path: str = os.environ.get("HOME") + "/Downloads/instantclient_19_8"


class WriterUserException(Exception):
    def __init__(self, *args, db_error: Optional[oracledb.DatabaseError] = None):
        self.db_error = db_error
        super().__init__(*args)


class OracleWriter:

    def __init__(self, oracle_credentials: OracleCredentials, log_folder: str,
                 sql_loader_options: SQLLoaderOptions,
                 default_format: DefaultFormatOptions,
                 sql_loader_path: str = 'sqlldr',
                 load_batch_size: int = 5000,
                 verbose_logging: bool = False, db_trace_enabled=False):
        self.__credentials = oracle_credentials
        self._logger = self._set_logger(log_folder, verbose_logging)
        self._connection = OracleConnection(**asdict(self.__credentials),
                                            logger=__name__)
        self._metadata_provider = OracleMetadataProvider(self._connection)
        self._sql_loader_options = sql_loader_options

        self._sql_loader = SQLLoaderExecutor(self._connection.dsn,
                                             oracle_credentials.username,
                                             oracle_credentials.password,
                                             global_format=default_format,
                                             log_folder=log_folder,
                                             sql_loader_path=sql_loader_path)
        self.log_folder = log_folder
        self._batch_size = load_batch_size
        self.trace_enabled = db_trace_enabled
        self._ext_session_id = ''
        self._default_format = default_format

    def connect(self, ext_session_id: str = ''):
        self._logger.debug("Connecting to database.")
        try:
            self._connection.connect()
            self._logger.info("Setting default NLS session.")
            self._set_default_session()
        except DatabaseError as e:
            raise WriterUserException(f"Login to database failed, please check your credentials. Detail: {e}") from e

        self._ext_session_id = ext_session_id
        if self.trace_enabled:
            self._enable_db_trace()
            self._logger.debug(f"DB Trace enabled and outputting to: {self.get_trace_file_path()}")

    def test_connection(self):
        self.connect()
        self.close_connection()

    def _set_default_session(self):
        self.execute_script("alter session set NLS_NUMERIC_CHARACTERS = '. '")
        self.execute_script(f"alter session set NLS_TIMESTAMP_FORMAT = '{self._default_format.timestamp_format}'")
        self.execute_script(f"alter session set NLS_DATE_FORMAT = '{self._default_format.date_format}'")

    def close_connection(self):
        self._logger.debug("Closing the connection.")
        self._connection.connection.close()
        if self.trace_enabled:
            self._disable_db_trace()

    def _enable_db_trace(self):
        sid, serial = self._connection.get_session_id()
        query = f"""
        begin
          dbms_monitor.session_trace_enable (
            session_id => {sid}, 
            serial_num => {serial}, 
            waits      => true, 
            binds      => true,
            plan_stat  => 'all_executions');
        end;
        """  # noqa
        res = self._connection.perform_query(query)
        list(res)
        self.trace_enabled = True

    def _disable_db_trace(self):
        sid, serial = self._connection.get_session_id()
        query = f"""
        begin
          dbms_monitor.session_trace_disable (
            session_id => {sid}, 
            serial_num => {serial});
        end;
        """  # noqa
        res = self._connection.perform_query(query)
        list(res)
        self.trace_enabled = True

    def get_trace_file_path(self) -> str:
        if not self.trace_enabled:
            raise WriterUserException("The db trace is not enabled in the constructor")
        sid, serial = self._connection.get_session_id()

        query = f"""select
                   r.value                                ||'/diag/rdbms/'||
                   sys_context('USERENV','DB_NAME')       ||'/'||
                   sys_context('USERENV','INSTANCE_NAME') ||'/trace/'||
                   sys_context('USERENV','DB_NAME')       ||'_ora_'||p.spid||'.trc'
                   as tracefile_name
                from v$session s, v$parameter r, v$process p
                where r.name = 'diagnostic_dest'
                and s.sid = {sid}
                and p.addr = s.paddr
                """
        res = self._connection.perform_query(query)
        return list(res)[0][0]

    def _set_logger(self, log_folder: str, verbose: bool = False) -> logging.Logger:
        sql_log_path = Path(f'{log_folder}/writer_debug.log')
        sql_log_path.parent.mkdir(parents=True, exist_ok=True)

        class DebugFilter(logging.Filter):
            def filter(self, rec):
                return rec.levelno == logging.DEBUG

        handler = logging.handlers.WatchedFileHandler(sql_log_path, mode="w")
        # handler.addFilter(DebugFilter())
        formatter = logging.Formatter("[%(asctime)s]:  %(message)s")
        handler.setFormatter(formatter)
        logger = logging.getLogger(__name__)
        level = 'DEBUG' if verbose else 'INFO'
        logger.setLevel(level)
        logger.addHandler(handler)

        return logger

    def execute_script(self, script: str, continue_on_failure: bool = False):
        res = self._connection.perform_query(script)
        return list(res)

    def upload_full(self, data_path: str, schema: str, table_name: str, columns: List[str],
                    pre_procedure: Optional[str] = None, pre_procedure_parameters: Optional[list] = None):

        table_metadata = self._metadata_provider.get_table_metadata(schema, table_name)
        self._validate_schema(columns, table_metadata.columns)

        sql_loader_mode = 'REPLACE'
        if pre_procedure:
            self._logger.info(f"Running PRE procedure '{pre_procedure}' with parameters {pre_procedure_parameters}")
            self._connection.run_procedure(pre_procedure, pre_procedure_parameters)
            # the procedure is expected to empty the table
            sql_loader_mode = 'INSERT'
        self._logger.info(f"Inserting data in full mode using SQL*Loader, mode: {sql_loader_mode}")
        self._load_data_into_table(data_path, schema, table_name, columns,
                                   table_metadata.columns,
                                   method='sqlldr',
                                   mode=sql_loader_mode)

    def upload_incremental(self, data_path: str, schema: str, table_name: str, columns: List[str],
                           primary_key: Optional[List[str]] = None,
                           method: Literal['query', 'sqlldr'] = 'sqlldr'):
        """
        Perform upsert or append if no primary key is defined.

        Args:
            data_path:
            schema:
            table_name:
            columns:
            primary_key:
            method: Literal['query', 'sqlldr']: data load method

        Returns:

        """
        self._logger.debug(f"Getting metadata for table: {schema}.{table_name}")
        table_metadata = self._metadata_provider.get_table_metadata(schema, table_name)

        self._validate_schema(columns, table_metadata.columns)
        target_table_name = self._build_table_identifier(schema, table_name)
        if primary_key and method == 'query':
            # upsert mode
            try:
                self._perform_upsert(data_path, table_name, target_table_name, columns, primary_key, table_metadata,
                                     method=method)
            except Exception as e:
                # always drop temp table
                self._drop_temp_table(table_name)
                raise e
        else:
            # append mode
            self._load_data_into_table(data_path, schema, table_name, columns, table_metadata.columns,
                                       method=method,
                                       mode='APPEND')

    def _build_table_identifier(self, schema: str | None, table_name: str):
        target_table_name = self._connection.escape(table_name)
        if schema:
            target_table_name = f'{self._connection.escape(schema)}.{target_table_name}'

        return target_table_name

    def _perform_upsert(self, data_path: str, table_name: str, target_table_name: str,
                        columns: List[str], primary_key: List[str], table_metadata: TableSchema,
                        method: Literal['query', 'sqlldr']):
        temp_table_name = self._create_temp_table(table_name, table_metadata.columns)

        self._load_data_into_table(data_path, None, temp_table_name, columns, table_metadata.columns, method=method)

        escape = self._connection.escape
        join_clause = ' AND '.join([f'a.{escape(col)}=b.{escape(col)}' for col in primary_key])

        update_clause = ', '.join([f'a.{escape(col)}=b.{escape(col)}' for col in columns if col not in primary_key])

        insert_clause = ', '.join(columns)
        insert_values_clause = ', '.join([f'b.{escape(col)}' for col in columns])

        merge_query = f"""MERGE INTO {target_table_name} a
                                    USING (SELECT * FROM {temp_table_name}) b
                                    ON ({join_clause})
                                    WHEN MATCHED THEN UPDATE SET {update_clause}
                                    WHEN NOT MATCHED THEN INSERT ({insert_clause}) VALUES ({insert_values_clause})
                                    """

        res = self._connection.perform_query(merge_query)
        list(res)
        # TODO: Is it necessary to commit, if so when?
        self._logger.debug("Executing Commit")
        self._connection.connection.commit()

        drop_query = f"DROP TABLE {temp_table_name}"
        self._logger.info("Removing temporary table")
        res = self._connection.perform_query(drop_query)
        list(res)

    def _create_temp_table(self, table_name: str, columns: List[ColumnSchema]) -> str:

        column_signatures = [f'{self._connection.escape(col.name)} {col.source_type_signature}' for col in columns]

        temp_table_name = self._get_temp_table_name(table_name)
        query = f"""CREATE TABLE {temp_table_name}
                    ({', '.join(column_signatures)})
        """
        self._logger.debug("Creating temporary table.")
        try:
            res = self._connection.perform_query(query)
            # just trigger the results
            list(res)
        except WriterUserException as e:
            if e.db_error.full_code == 'ORA-00955':
                # table already exists error skipped
                self._logger.debug(f"Temporary table {temp_table_name} already exists!")
            else:
                raise e

        return temp_table_name

    def _drop_temp_table(self, table_name: str) -> str:

        temp_table_name = self._get_temp_table_name(table_name)
        query = f"DROP TABLE {temp_table_name}"
        self._logger.debug("Dropping temporary table.")
        try:
            res = self._connection.perform_query(query)
            # just trigger the results
            list(res)
        except WriterUserException as e:
            if e.db_error.full_code == 'ORA-00955':
                # table already exists error skipped
                self._logger.debug(f"Temporary table {temp_table_name} already exists!")
            elif e.db_error.full_code == 'ORA-00942':
                # table does not exist error skipped
                self._logger.debug(f"Temporary table {temp_table_name} does not exist!")
            else:
                raise e

        return temp_table_name

    def _get_temp_table_name(self, table_name):
        temp_table_name = f'KBC_TMP_{self._ext_session_id}_{table_name.upper()}'
        return temp_table_name

    def _load_data_into_table(self, data_path: str, schema: str | None, table_name: str, columns: List[str],
                              destination_schema: List[ColumnSchema],
                              method: Literal['sqlldr', 'query'] = 'sqlldr', mode='INSERT'):
        # important to order by CSV column order
        indexed_schema = {col.name: col for col in destination_schema}
        columns_involved = [indexed_schema[col] for col in columns]

        if method == 'sqlldr':
            self._logger.info(f"Running load mode: {method}")
            table_identifier = self._build_table_identifier(schema, table_name)
            columns_types = self._get_sqlldr_types(columns_involved)
            self._sql_loader.load_data(data_path, table_identifier, columns_types,
                                       mode=mode, errors=0,
                                       **asdict(self._sql_loader_options))
        elif method == 'query':
            self._logger.info(f"Running load mode: '{method}'")
            self._insert_records_query(data_path, schema, table_name, columns)

    def _insert_records_query(self, data_path: str, schema: str, table_name: str, columns: List[str],
                              skip_first_line: bool = True):
        cursor = self._connection.connection.cursor()
        # Predefine the memory areas to match the table definition
        # cursor.setinputsizes(None, 25)

        table_identifier = self._build_table_identifier(schema, table_name)
        values_clause = ', '.join([f':{i}' for i, col in enumerate(columns)])
        columns_clause = ', '.join([col for col in columns])
        insert_query = f"INSERT INTO {table_identifier} ({columns_clause}) VALUES ({values_clause})"

        self._logger.debug(f"Executing insert queries with parameters: batch_size={self._batch_size}")
        self._logger.debug(f"Insert query template: {insert_query}")

        with open(data_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            buffer = []
            if skip_first_line:
                csv_file.readline()
            for line in csv_reader:
                buffer.append(line)
                if len(buffer) % self._batch_size == 0:
                    cursor.executemany(insert_query, buffer)
                    buffer = []
            if buffer:
                cursor.executemany(insert_query, buffer)

        cursor.close()
        # TODO: Is it necessary to commit, if so when?
        self._logger.debug("Executing Commit")
        self._connection.connection.commit()

    def _validate_schema(self, columns: List[str], destination_columns: List[ColumnSchema]):
        expected_names = [col.name for col in destination_columns]
        mismatched = [col for col in columns if col not in expected_names]
        if mismatched:
            raise WriterUserException(f"The destination schema does not contain specified columns: {mismatched}. "
                                      f"The expected schema is: {expected_names}. "
                                      "Please check the column mapping and case")

    def _get_sqlldr_types(self, columns_involved: List[ColumnSchema]) -> List[Tuple[str, str]]:
        """
        Returns column names with SQLLoader type annotations
        Args:
            columns_involved:

        Returns:

        """
        result_typed = list()
        for c in columns_involved:
            col_type = c.source_type.split('(')[0]
            # Use only dates
            sqlldr_type = ''
            if col_type.upper() in ['TIMESTAMP', 'DATE']:
                sqlldr_type = col_type
            if 'CHAR' in col_type.upper():
                sqlldr_type = f'CHAR({c.length})'
            result_typed.append((c.name, sqlldr_type))

        return result_typed
