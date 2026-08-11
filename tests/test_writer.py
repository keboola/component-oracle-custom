import logging
import shutil
import tempfile
import unittest

import mock
import oracledb

from configuration import DefaultFormatOptions, SQLLoaderOptions
from db_writer.table_schema import ColumnSchema
from db_writer.writer import OracleCredentials, OracleWriter, WriterUserException


class FakeOracleError:
    """Minimal stand-in for the error object oracledb carries in DatabaseError.args."""

    def __init__(self, message: str, full_code: str):
        self.message = message
        self.full_code = full_code


class TestQueryLoadErrorHandling(unittest.TestCase):
    """Covers the 'query' (INSERT) load method error handling in OracleWriter._load_data_into_table."""

    DESTINATION_SCHEMA = [ColumnSchema(name='ID', source_type='NUMBER', source_type_signature='NUMBER(10) NOT NULL')]

    def setUp(self):
        self._log_folder = tempfile.mkdtemp()
        # OracleWriter attaches a file handler to the module logger, keep the global state clean
        self._logger = logging.getLogger('db_writer.writer')
        self._original_handlers = list(self._logger.handlers)
        self._original_level = self._logger.level

    def tearDown(self):
        for handler in list(self._logger.handlers):
            if handler not in self._original_handlers:
                handler.close()
                self._logger.removeHandler(handler)
        self._logger.setLevel(self._original_level)
        shutil.rmtree(self._log_folder, ignore_errors=True)

    def _build_writer(self) -> OracleWriter:
        credentials = OracleCredentials(username='user', password='pass', host='localhost', port=1521,
                                        service_name='xe', insta_client_path='/tmp/instantclient')
        return OracleWriter(credentials,
                            log_folder=self._log_folder,
                            sql_loader_options=SQLLoaderOptions(),
                            default_format=DefaultFormatOptions())

    def _load_with_insert_failure(self, exception: BaseException):
        writer = self._build_writer()
        with mock.patch.object(writer, '_insert_records_query', side_effect=exception):
            writer._load_data_into_table('/dev/null', 'SOME_SCHEMA', 'SOME_TABLE', ['ID'],
                                         self.DESTINATION_SCHEMA, method='query')

    def test_integrity_error_is_reported_as_user_exception(self):
        """A constraint violation is the user's to fix, so it must fail as a user error, not an internal one."""
        # oracledb appends a "Help: <url>" line to the error message, so the Oracle detail goes last
        db_error = FakeOracleError('ORA-00001: unique constraint (SOME_SCHEMA.PK_SOME_TABLE) violated\n'
                                   'Help: https://docs.oracle.com/error-help/db/ora-00001/', 'ORA-00001')

        with self.assertRaises(WriterUserException) as context:
            self._load_with_insert_failure(oracledb.IntegrityError(db_error))

        message = str(context.exception)
        self.assertTrue(message.startswith('The destination table rejected the loaded data.'), message)
        self.assertIn('duplicate values in unique or primary key columns', message)
        self.assertIn('Oracle error: ORA-00001: unique constraint', message)
        self.assertIs(db_error, context.exception.db_error)

    def test_integrity_error_without_error_object_still_reports_user_exception(self):
        """The handler itself must never crash, that would put the job back on an internal error."""
        with self.assertRaises(WriterUserException) as context:
            self._load_with_insert_failure(oracledb.IntegrityError())

        self.assertIn('The destination table rejected the loaded data', str(context.exception))
        self.assertIsNone(context.exception.db_error)

    def test_non_integrity_database_error_is_not_reclassified(self):
        """Anything that is not a constraint violation must keep propagating untouched."""
        original = oracledb.DatabaseError('DPY-4011: the database or network closed the connection')

        with self.assertRaises(oracledb.DatabaseError) as context:
            self._load_with_insert_failure(original)

        self.assertIs(original, context.exception)

    def test_successful_query_load_is_untouched(self):
        """The happy path must not be affected by the error handling."""
        writer = self._build_writer()

        with mock.patch.object(writer, '_insert_records_query') as insert_records_query:
            writer._load_data_into_table('/dev/null', 'SOME_SCHEMA', 'SOME_TABLE', ['ID'],
                                         self.DESTINATION_SCHEMA, method='query')

        insert_records_query.assert_called_once_with('/dev/null', 'SOME_SCHEMA', 'SOME_TABLE', ['ID'])


if __name__ == "__main__":
    unittest.main()
