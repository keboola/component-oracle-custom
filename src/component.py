"""
Template Component main class.

"""
import logging

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
import configuration
from db_writer.writer import OracleWriter, OracleCredentials

INSTA_CLIENT_PATH = '/usr/local/instantclient_21_8'

SQLLDR_PATH = '/usr/local/instantclient_21_8/sqlldr'


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

        self._init_configuration()
        self._init_writer_client()

        if not self.get_input_tables_definitions():
            raise UserException("No input table specified. Please provide one input table in the input mapping!")
        input_table = self.get_input_tables_definitions()[0]

        load_type = self._configuration.loading_options.load_type

        if load_type == 'full_load':
            mode = self._configuration.loading_options.full_load_mode
            self._oracle_writer.upload_full(input_table.full_path,
                                            schema=self._configuration.schema,
                                            table_name=self._configuration.table_name,
                                            columns=input_table.columns)
        elif load_type == 'incremental':
            self._oracle_writer.upload_incremental(input_table.full_path,
                                                   schema=self._configuration.schema,
                                                   table_name=self._configuration.table_name,
                                                   columns=input_table.columns,
                                                   primary_key=input_table.primary_key
                                                   )

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
        self._oracle_writer = OracleWriter(credentials, log_folder='./temp/log',
                                           sql_loader_path=sql_loader_path,
                                           verbose_logging=self._configuration.debug)
        self._oracle_writer.connect()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
