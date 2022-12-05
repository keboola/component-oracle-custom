"""
Template Component main class.

"""
import logging
import os
from typing import List

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
import configuration
from db_writer.sql_loader import SQLLoaderException
from db_writer.writer import OracleWriter, OracleCredentials, WriterUserException

INSTA_CLIENT_PATH = os.environ.get('ORACLE_INSTANT_CLI_PATH', '/usr/local/instantclient_21_8')

SQLLDR_PATH = os.environ.get('SQLLOADER_PATH', '/usr/local/instantclient_21_8/sqlldr')


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self._configuration: configuration.Configuration
        self._oracle_writer: OracleWriter

    def run(self):
        """
        Main execution code
        """
        self._init_loggers()
        self._init_configuration()
        self._validate_host_names()
        self._init_writer_client()

        if not self.get_input_tables_definitions():
            raise UserException("No input table specified. Please provide one input table in the input mapping!")
        input_table = self.get_input_tables_definitions()[0]
        loading_options = self._configuration.loading_options
        load_type = loading_options.load_type

        columns = self._map_columns(input_table.columns)

        if self._configuration.pre_run_scripts and self._configuration.pre_run_scripts.script:
            logging.info(f"Pre script detected, running: {self._configuration.pre_run_scripts.script}")
            self._oracle_writer.execute_script(self._configuration.pre_run_scripts.script,
                                               self._configuration.pre_run_scripts.continue_on_failure)

        if load_type == 'full_load':
            pre_procedure = loading_options.full_load_procedure
            pre_procedure_params = loading_options.full_load_procedure_parameters_list
            self._oracle_writer.upload_full(input_table.full_path,
                                            schema=self._configuration.schema,
                                            table_name=self._configuration.table_name,
                                            columns=columns,
                                            pre_procedure=pre_procedure,
                                            pre_procedure_parameters=pre_procedure_params)
        elif load_type == 'incremental':
            self._oracle_writer.upload_incremental(input_table.full_path,
                                                   schema=self._configuration.schema,
                                                   table_name=self._configuration.table_name,
                                                   columns=columns,
                                                   primary_key=input_table.primary_key,
                                                   method=loading_options.incremental_load_mode
                                                   )

        if self._configuration.post_run_scripts and self._configuration.post_run_scripts.script:
            logging.info(f"Post script detected, running: {self._configuration.post_run_scripts.script}")
            self._oracle_writer.execute_script(self._configuration.post_run_scripts.script,
                                               self._configuration.post_run_scripts.continue_on_failure)

    def _validate_host_names(self):
        approved_hostnames = self.configuration.image_parameters.get("approved_hostnames")
        host = self._configuration.db.host
        port = self._configuration.db.port
        if approved_hostnames:
            valid_host = any([(host == h['host'] and str(port) == str(h['port']))
                              for h in approved_hostnames])

            if not valid_host:
                raise UserException(f'Hostname "{host}" with port "{port}" is not approved.')

    def _init_loggers(self):
        class DebugFilter(logging.Filter):
            def filter(self, rec):
                return not (rec.levelno == logging.DEBUG and rec.name == 'db_writer.writer')

        if self.configuration.parameters.get('debug', False):
            # let db_writer handle the debug logging in debug mode
            for h in logging.getLogger().handlers:
                h.addFilter(DebugFilter())

    def _init_configuration(self):
        self.validate_configuration_parameters(configuration.Configuration.get_dataclass_required_parameters())
        self._configuration: configuration.Configuration = configuration.Configuration.load_from_dict(
            self.configuration.parameters)

    def _init_writer_client(self):
        # build credentials
        db_config = self._configuration.db
        credentials = OracleCredentials(username=db_config.user,
                                        password=db_config.pswd_password,
                                        insta_client_path=INSTA_CLIENT_PATH,
                                        host=db_config.host, port=db_config.port, service_name=db_config.database)
        sql_loader_path = SQLLDR_PATH
        self._oracle_writer = OracleWriter(credentials, log_folder=self.files_out_path,
                                           sql_loader_path=sql_loader_path,
                                           sql_loader_options=self._configuration.sql_loader_options,
                                           verbose_logging=self._configuration.debug)
        self._oracle_writer.connect(ext_session_id=self.environment_variables.run_id)

    def _map_columns(self, columns: List[str]) -> List[str]:
        if not self._configuration.columns:
            return columns

        invalid_mapping: List[str] = list()
        for mapping in self._configuration.columns:
            if mapping.source_name not in columns:
                invalid_mapping.append(mapping.source_name)

            idx = columns.index(mapping.source_name)
            columns[idx] = mapping.destination_name

        if invalid_mapping:
            raise UserException(f"Some source column names in mapping do not exist in the source table: "
                                f"{invalid_mapping}")
        return columns


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except (UserException, WriterUserException, SQLLoaderException) as exc:
        detail = ''
        if len(exc.args) > 1:
            # remove extra argument to make logging.exception log properly
            detail = exc.args[1]
            exc.args = exc.args[:1]
        logging.exception(exc, extra={"additional_detail": detail})
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
