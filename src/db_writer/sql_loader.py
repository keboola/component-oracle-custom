import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Literal, List, Tuple

from configuration import DefaultFormatOptions


class SQLLoaderException(Exception):
    pass


class CTLFileBuilder:
    CTLLoadMode = Literal['INSERT', 'TRUNCATE', 'REPLACE']

    @staticmethod
    def _load_data_into(table_name: str):
        return f'load data into table {table_name}'

    @staticmethod
    def _fields_terminated_by(terminator: str, enclosure: str = '\\"'):
        return f'fields terminated by "{terminator}" OPTIONALLY ENCLOSED BY "{enclosure}"'

    @staticmethod
    def _column_list(columns: List[Tuple[str, str]]):
        column_strings = [f"{c[0]}{' ' + c[1] if c[1] else ''}" for c in columns]
        column_expression = ", \n ".join(column_strings)
        return f'({column_expression})'

    @staticmethod
    def _mode(mode: CTLLoadMode):
        return mode

    @staticmethod
    def _date_default_format(_format: str):
        return f'DATE FORMAT "{_format}"'

    @staticmethod
    def _timestamp_default_format(_format: str):
        return f'TIMESTAMP FORMAT "{_format}"'

    @classmethod
    def build(cls, table_name: str,
              columns: List[Tuple[str, str]],
              mode: CTLLoadMode = 'INSERT',
              default_format: DefaultFormatOptions = None,
              field_delimiter: str = ',') -> Path:
        """
        Builds SQL loader control file in temporary location.

        Args:
            table_name:
            columns: List[Tuple[str, str]]: Column name / Optional Type definition tuple,
                e.g. [('COLA', "DATE YY_MM_DD")]
            mode:
            default_format:
            field_delimiter:

        Returns: Result file path

        """

        fd, ctl_path = tempfile.mkstemp(suffix='sqlldr.ctl')

        with os.fdopen(fd, 'w', encoding='utf-8') as out:
            out.write(cls._load_data_into(table_name))
            out.write('\n')
            out.write(cls._mode(mode))
            out.write('\n')
            out.write(cls._fields_terminated_by(field_delimiter))
            out.write('\n')
            if default_format:
                out.write(cls._date_default_format(default_format.date_format))
                out.write('\n')
                out.write(cls._timestamp_default_format(default_format.timestamp_format))
                out.write('\n')
            out.write(cls._column_list(columns))

        return Path(ctl_path)


class SQLLoaderExecutor:

    def __init__(self, host_string: str, user: str, password: str,
                 global_format: DefaultFormatOptions = None,
                 sql_loader_path: str = 'sqlldr',
                 log_folder: str = './temp/logs'):
        """

        Args:
            sql_loader_path: Path or command name to sql loader executable (sqlldr)
            log_folder:
            global_format: dict: GLOBAL format masks
                e.g. {'date_format': 'YY-MM-DD',
                      'timestamp_format': 'YYYY-MM-DD HH24:MI:SS.FF6'}
        """
        self.host = host_string
        self._sql_loader_path = sql_loader_path
        self._log_folder = log_folder
        self._global_format = global_format
        self.__user = user
        self.__password = password

    @property
    def _uid_string(self) -> str:
        uid = f"{self.__user}/{self.__password}@{self.host}"

        # needed for SYS user
        if self.__user.lower() == 'sys':
            uid += ' AS SYSDBA'

        # wrap in quotes
        return f"'{uid}'"

    def _prepare_log_folder(self):
        if not os.path.exists(self._log_folder):
            os.makedirs(self._log_folder, exist_ok=True)

    def load_data(self, data_path: str,
                  table_name: str,
                  columns: List[Tuple[str, str]],
                  skip_first_line: bool = True,
                  mode: Literal['INSERT', 'TRUNCATE', 'REPLACE'] = 'INSERT',
                  field_delimiter: str = ',',
                  errors: int = 50,
                  rows: int = 5000,
                  bindsize: int = 8000000,
                  **kwargs):
        """

        Performs sqlldr command to load data.

        Additional parameters:
         control -- control file name

          discard -- discard file name
          discardmax -- number of discards to allow          (Default all)
          skip -- number of logical records to skip    (Default 0)
          load -- number of logical records to load    (Default all)
          errors -- number of errors to allow            (Default 50)
          rows -- number of rows in conventional path bind array or between direct
    path data saves
                   (Default: Conventional path 64, Direct path all)
         bindsize -- size of conventional path bind array in bytes  (Default 256000)
        silent -- suppress messages during run (header,feedback,errors,discards,
    partitions)
         direct -- use direct path                      (Default FALSE)
         parallel -- do parallel load                     (Default FALSE)
          file -- file to allocate extents from
        skip_unusable_indexes -- disallow/allow unusable indexes or index partitions
        (Default FALSE)
        skip_index_maintenance -- do not maintain indexes, mark affected indexes as
        unusable  (Default FALSE)
        commit_discontinued -- commit loaded rows when load is discontinued  (Default
        FALSE)
          readsize -- size of read buffer                  (Default 1048576)
        external_table -- use external table for load; NOT_USED, GENERATE_ONLY, EXECUTE
         (Default NOT_USED)
        columnarrayrows -- number of rows for direct path column array  (Default 5000)
        streamsize -- size of direct path stream buffer in bytes  (Default 256000)
        multithreading -- use multithreading in direct path
        resumable -- enable or disable resumable for current session  (Default FALSE)
        resumable_name -- text string to help identify resumable statement
        resumable_timeout -- wait time (in seconds) for RESUMABLE  (Default 7200)
        date_cache -- size (in entries) of date conversion cache  (Default 1000)

        Args:

            data_path: Path of input CSV file
            table_name:
            columns: List[Tuple[str, str]]: Column name / Optional Type definition tuple,
                e.g. [('COLA', "DATE YY_MM_DD")]
            skip_first_line: Flag whether to skip first line (header)
            mode:
            field_delimiter:
            errors:
            rows:
            bindsize:
            **kwargs:

        Returns:

        """
        self._prepare_log_folder()
        ctl_file_path = CTLFileBuilder.build(table_name, columns, mode, self._global_format, field_delimiter)
        skip = 1 if skip_first_line else 0
        logging.info(f"Sqlldr control file \n: {open(ctl_file_path, 'r').read()}")
        parameters = {
            "userid": self._uid_string,
            "control": ctl_file_path,
            "data": data_path,
            "bad": self.bad_log_path,
            "log": self.log_file_path,
            "errors": errors,
            "rows": rows,
            "skip": skip,
            "bindsize": bindsize
        }

        parameters = {**parameters, **kwargs}
        self._execute_sqlloader(parameters)

    @property
    def bad_log_path(self) -> str:
        return Path(f'{self._log_folder}/bad.log').as_posix()

    @property
    def log_file_path(self) -> str:
        return Path(f'{self._log_folder}/log.log').as_posix()

    @staticmethod
    def _build_args_from_dict(parameters: dict):
        args = [f"{key}={value}" for key, value in parameters.items()]
        return args

    def _execute_sqlloader(self, parameters: dict):
        additional_args = self._build_args_from_dict(parameters)
        args = [self._sql_loader_path] + additional_args
        process = subprocess.Popen(args,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        logging.debug(f'Running SQL loader command: {args}', extra={'additional_detail': stdout})
        process.poll()
        if process.poll() != 0:
            full_log = open(self.log_file_path, 'r').read()
            raise SQLLoaderException(f'Failed to execute the SQL*Loader script. Log in event detail. {stderr}',
                                     full_log)
        elif stderr:
            logging.warning(stderr)
