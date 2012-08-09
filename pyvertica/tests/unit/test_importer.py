import unittest2 as unittest

from mock import Mock, call, patch

from pyvertica.importer import (
    AlreadyImportedError,
    BaseImporter,
    BatchImportError,
)


class BaseImporterTestCase(unittest.TestCase):
    """
    Tests for :py:class:`.BaseImporter`.
    """
    def setUp(self):
        self.mapping_list = (
            {
                'field_name': 'field_1',
                'db_data_type': 'VARCHAR(10)',
            },
            {
                'field_name': 'field_2',
                'db_data_type': 'VARCHAR(20)',
                'db_field_name': 'db_field_2',
            },
            {
                'field_name': 'field_3',
                'db_data_type': 'VARCHAR(30)',
            }
        )

    def get_importer(self, **kwargs):
        arguments = {
            'reader_obj': Mock(),
            'dsn': 'TestDSN',
            'schema_name': 'schema',
            'batch_source_path': 'test/path',
        }
        arguments.update(kwargs)
        return BaseImporter(**arguments)

    def test___init__(self):
        """
        Test initialization of :py:class:`.BaseImporter`.
        """
        reader_obj = Mock()

        importer = self.get_importer(reader_obj=reader_obj, foo='bar')

        self.assertEqual(reader_obj, importer._reader_obj)
        self.assertEqual('TestDSN', importer._dsn)
        self.assertEqual('schema', importer._schema_name)
        self.assertEqual({
            'foo': 'bar',
            'batch_source_path': 'test/path',
        }, importer._kwargs)

    @patch('pyvertica.importer.BaseImporter._get_db_column_list')
    @patch('pyvertica.importer.VerticaBatch')
    def test__get_vertica_batch(self, VerticaBatch, get_db_column_list):
        """
        Test :py:meth:`.BaseImporter._get_vertica_batch`.
        """
        importer = self.get_importer()
        importer.table_name = 'test_table'

        batch_obj = importer._get_vertica_batch()

        self.assertEqual(VerticaBatch.return_value, batch_obj)
        VerticaBatch.assert_called_once_with(
            dsn='TestDSN',
            table_name='schema.test_table',
            column_list=get_db_column_list.return_value,
        )

    def test__get_db_column_list(self):
        """
        Test :py:meth:`.BaseImporter._get_db_column_list`.
        """
        importer = self.get_importer()
        importer.mapping_list = self.mapping_list
        importer.extra_fields = (
            {'field_name': 'extra_1'},
            {'field_name': 'extra_2'},
        )

        self.assertEqual(
            ['extra_1', 'extra_2', 'field_1', 'db_field_2', 'field_3'],
            importer._get_db_column_list()
        )

    def test__get_row_value_list(self):
        """
        Test :py:meth:`.BaseImporter._get_row_value_list`.
        """
        importer = self.get_importer()
        importer.mapping_list = self.mapping_list
        importer.extra_fields = (
            {'field_name': 'extra1'},
            {'field_name': 'extra2'},
        )
        importer.get_extra_extra1_data = Mock(return_value='extra_data1')
        importer.get_extra_extra2_data = Mock(return_value='extra_data2')

        row_value_list = importer._get_row_value_list({
            'field_1': 'data1',
            'field_2': 'data2',
            'field_3': 'data3',
            'field_4': 'data4',
        })

        self.assertEqual(
            ['extra_data1', 'extra_data2', 'data1', 'data2', 'data3'],
            row_value_list
        )

    def test__insert_into_history(self):
        """
        Test :py:meth:`.BaseImporter._insert_into_history`.
        """
        importer = self.get_importer()
        importer.batch_source_name = Mock()
        importer.batch_source_type_name = Mock()
        importer.get_extra_batch_source_path_data = Mock()
        importer.get_extra_batch_import_timestamp_data = Mock()

        db_cursor = Mock()

        importer._insert_into_history(db_cursor)

        db_cursor.execute.assert_called_once_with(
            'INSERT INTO meta.batch_history (batch_source_name, '
            'batch_source_type_name, batch_source_path, '
            'batch_import_timestamp) VALUES (?, ?, ?, ?)',
            importer.batch_source_name,
            importer.batch_source_type_name,
            importer.get_extra_batch_source_path_data.return_value,
            importer.get_extra_batch_import_timestamp_data.return_value,
        )

    def test_get_sql_create_table_statement(self):
        """
        Test :py:meth:`.BaseImporter.get_sql_create_table_statement`.
        """
        importer = self.get_importer()
        importer.mapping_list = self.mapping_list
        importer.extra_fields = (
            {
                'field_name': 'extra_1',
                'db_data_type': 'INTEGER',
            },
            {
                'field_name': 'extra_2',
                'db_data_type': 'VARCHAR(25)',
            }
        )
        importer.table_name = 'test_table'

        self.assertEqual(
            'CREATE TABLE schema.test_table (extra_1 INTEGER, extra_2 '
            'VARCHAR(25), field_1 VARCHAR(10), db_field_2 VARCHAR(20), '
            'field_3 VARCHAR(30))',
            importer.get_sql_create_table_statement()
        )

    def test_start_import(self):
        """
        Test :py:meth:`.BaseImporter.start_import`.
        """
        batch_obj = Mock()
        batch_obj.get_errors.return_value = (False, Mock())
        batch_obj.get_cursor = Mock()

        importer = self.get_importer(reader_obj=[1, 2, 3])
        importer.get_batch_source_path_exists = Mock(return_value=False)
        importer._get_row_value_list = Mock(side_effect=['a', 'b', 'c'])
        importer._get_vertica_batch = Mock(return_value=batch_obj)
        importer._insert_into_history = Mock()

        importer.start_import()

        importer._get_vertica_batch.assert_called_once_with()
        importer.get_batch_source_path_exists.assert_called_once_with(
            'TestDSN', 'test/path')
        self.assertEqual(
            [call(1), call(2), call(3)],
            importer._get_row_value_list.call_args_list
        )
        self.assertEqual(
            [call('a'), call('b'), call('c')],
            batch_obj.insert_list.call_args_list
        )
        batch_obj.get_errors.assert_called_once_with()
        batch_obj.get_cursor.assert_called_once_with()
        importer._insert_into_history.assert_called_once_with(
            batch_obj.get_cursor.return_value)
        batch_obj.commit.assert_called_once_with()

    @patch('pyvertica.importer.logger')
    def test_start_import_errors(self, logger):
        """
        Test :py:meth:`.BaseImporter.start_import` with errors.
        """
        batch_obj = Mock()
        batch_obj.get_errors.return_value = (True, ['Error 1\n', 'Error 2\r'])

        importer = self.get_importer(reader_obj=[])
        importer._kwargs['batch_source_path'] = 'test/path'
        importer.get_batch_source_path_exists = Mock(return_value=False)
        importer.mapping_list = self.mapping_list
        importer._get_vertica_batch = Mock(return_value=batch_obj)

        self.assertRaises(BatchImportError, importer.start_import)

        self.assertEqual(0, batch_obj.commit.call_count)

        self.assertEqual([
            call('Batch error (test/path): Error 1'),
            call('Batch error (test/path): Error 2'),
        ], logger.error.call_args_list)

    def test_start_import_already_imported(self):
        """
        Test :py:meth:`.BaseImporter.start_import` with already imported record
        """
        importer = self.get_importer()
        importer.get_batch_source_path_exists = Mock(return_value=True)
        importer._kwargs['batch_source_path'] = 'test/path'

        self.assertRaises(AlreadyImportedError, importer.start_import)

    @patch('pyvertica.importer.BaseImporter.batch_source_name', 'test_bsn')
    @patch('pyvertica.importer.BaseImporter.batch_source_type_name', 'ga3')
    @patch('pyvertica.importer.get_connection')
    def test_get_batch_source_path_exists_true(self, get_connection):
        """
        Test :py:meth:`.BaseImporter.get_batch_source_path_exists`.

        This tests the case of ``True``.
        """
        connection = get_connection.return_value
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = ['test/path']

        self.assertTrue(
            BaseImporter.get_batch_source_path_exists(
                'TestDSN', 'test/path')
        )

        get_connection.assert_called_once_with('TestDSN')
        connection.cursor.assert_called_once_with()
        cursor.execute.assert_called_once_with(
            'SELECT batch_source_path FROM meta.batch_history '
            'WHERE batch_source_name = ? AND batch_source_type_name = ? AND '
            'batch_source_path = ? LIMIT 1',
            'test_bsn',
            'ga3',
            'test/path'
        )
        cursor.fetchone.assert_called_once_with()

    @patch('pyvertica.importer.get_connection')
    def test_get_batch_source_path_exists_false(self, get_connection):
        """
        Test :py:meth:`.BaseImporter.get_batch_source_path_exists`.

        This tests the case of ``False``.
        """
        connection = get_connection.return_value
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = None

        self.assertFalse(
            BaseImporter.get_batch_source_path_exists(
                'TestDSN', 'test/path')
        )

    @patch('pyvertica.importer.BaseImporter.batch_source_name', 'test_bsn')
    @patch('pyvertica.importer.BaseImporter.batch_source_type_name', 'ga3')
    @patch('pyvertica.importer.get_connection')
    def test_get_last_imported_batch_source_path(self, get_connection):
        """
        Test :py:meth:`.BaseImporter.get_last_imported_batch_souce_path`.
        """
        connection = get_connection.return_value
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = ('foo/bar',)

        self.assertEqual(
            'foo/bar',
            BaseImporter.get_last_imported_batch_source_path('TestDSN')
        )

        get_connection.assert_called_once_with('TestDSN')
        connection.cursor.assert_called_once_with()
        cursor.execute.assert_called_once_with(
            'SELECT batch_source_path FROM meta.batch_history '
            'WHERE batch_source_name = ? AND batch_source_type_name = ? '
            'ORDER BY batch_import_timestamp DESC LIMIT 1',
            'test_bsn',
            'ga3'
        )

    @patch('pyvertica.importer.get_connection')
    def test_get_last_imported_batch_source_path_none(self, get_connection):
        """
        Test :py:meth:`.BaseImporter.get_last_imported_batch_source_path`.

        This tests the case of returning ``None``.
        """
        connection = get_connection.return_value
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = None

        self.assertEqual(
            None, BaseImporter.get_last_imported_batch_source_path('TestDSN'))

    def test_get_extra_batch_source_name_data(self):
        """
        Test :py:meth:`.BaseImporter.get_extra_batch_source_name_data`.
        """
        importer = self.get_importer()
        importer.batch_source_name = 'foo_bar'

        self.assertEqual(
            'foo_bar',
            importer.get_extra_batch_source_name_data({})
        )

    def test_get_extra_batch_source_path_data(self):
        """
        Test :py:meth:`.BaseImporter.get_extra_batch_source_path_data`.
        """
        importer = self.get_importer()
        importer._kwargs = {'batch_source_path': 'batch/source/path'}

        self.assertEqual(
            'batch/source/path', importer.get_extra_batch_source_path_data({}))

    @patch('pyvertica.importer.datetime')
    def test_get_extra_batch_import_timestamp_data(self, datetime):
        """
        Test :py:meth:`.BaseImporter.get_extra_batch_import_timestamp_data`.
        """
        utc_datetime = Mock()
        datetime.utcnow.return_value = utc_datetime

        importer = self.get_importer()

        self.assertEqual(None, importer._batch_import_timestamp)
        self.assertEqual(
            utc_datetime.isoformat.return_value,
            importer.get_extra_batch_import_timestamp_data({})
        )
        utc_datetime.isoformat.assert_called_once_with(' ')
        self.assertEqual(
            utc_datetime.isoformat.return_value,
            importer._batch_import_timestamp
        )
