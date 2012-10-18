import logging
from datetime import datetime

from pyvertica.connection import get_connection
from pyvertica.batch import VerticaBatch


logger = logging.getLogger(__name__)


class AlreadyImportedError(Exception):
    """
    Exception is raised when trying to import the same source twice.
    """
    pass


class BatchImportError(Exception):
    """
    Exception is raised when the batch import is returning errors.
    """
    pass


class BaseImporter(object):
    """
    Base class for importing data into Vertica.

    Note, before using this base importer, make sure you have created the
    history table for batch imports::

        CREATE TABLE meta.batch_history (
            batch_source_name VARCHAR(255),
            batch_source_type_name VARCHAR(255),
            batch_source_path VARCHAR(255),
            batch_import_timestamp TIMESTAMP
        )

    Usage example::

        class AdGroupPerformanceReportImporter(BaseImporter):
            table_name = 'adwords_ad_group_performance'
            batch_source_name = 'adwords_api'
            batch_source_type_name = 'ad_group_performance_report'

            mapping_list = (
                {
                    'field_name': 'AccountCurrencyCode',
                    'db_field_name': 'account_currency_code',
                    'db_data_type': 'VARCHAR(10)',
                },
                {
                    'field_name': 'AccountDescriptiveName',
                    'db_field_name': 'account_descriptive_name',
                    'db_data_type': 'VARCHAR(512)',
                },
                {
                    'field_name': 'AccountTimeZoneId',
                    'db_field_name': 'account_time_zone_id',
                    'db_data_type': 'VARCHAR(100)',
                },
                ...
            )

        iterable_object = [
            {
                'AccountCurrencyCode': 'EUR',
                'AccountDescriptiveName': 'Test account description',
                'AccountTimeZoneId': '(GMT+01:00) Amsterdam,'
            },
            ...
        ]

        report_importer = AdGroupPerformanceReportImporter(
            iterable_object,
            dsn='VerticaTST',
            schema_name='test',
            batch_source_path='ADGROUP_PERFORMANCE_REPORT.1234.20120521',
        )
        report_importer.start_import()

    In the example above, we are importing a ``list`` of dicts. More likely,
    this ``iterable_object`` would be a reader class for your data-source,
    which is iterable and would return a ``dict`` with the expected fields`.


    :param reader_obj:
        An object that is iterable and returns for every line the data as a
        ``dict``.

        .. note:: Passing a ``list`` of ``dict`` objects will work as well.

    :param dsn:
        ODBC data source name, used for connecting to the Vertica database.

    :param schema_name:
        Name of the DB schema to use.

    :param batch_source_path:
        A ``str`` describing the path to the source. This can be a file path
        when importing from a file, or an identifier when the source is an
        API. This should be unique for every import!

    :param kwargs:
        Optional extra keyword arguments, will be stored as ``self._kwargs``.

    """

    table_name = ''
    """
    Name of the database table (excluding the schema) (``str``).
    """

    batch_history_table = 'meta.batch_history'
    """
    Name of the database table containing the batch history (including the
    schema name) (``str``). The structure of this table is::

            batch_source_name VARCHAR(255)
            batch_source_type_name VARCHAR(255)
            batch_source_path VARCHAR(255)
            batch_import_timestamp TIMESTAMP

    """

    batch_source_name = ''
    """
    The name of the source which the data is retrieved from. E.g.: for AdWords,
    this this could be something like ``'adwords_api'``.
    """

    batch_source_type_name = ''
    """
    The type of data that is imported from the source. E.g.: for the AdWords
    API, this could be something like ``ADGROUP_PERFORMANCE_REPORT``.
    """

    mapping_list = ()
    """
    A ``tuple`` of ``dict`` objects to map record columns to db columns.

    The fields specified in this list might be a sub-set of the available
    fields in the record. Only specify the fields you want to store in the DB.

    Each ``dict`` must contain the following keys:

    ``field_name``
        A ``str`` representing the field name as it is in the ``dict``
        containing the data (as returned by the ``reader_obj``).

    ``db_data_type``
        A ``str`` representing the field type in the database,
        eg: ``'varchar(10)'``.

    Optionally, each ``dict`` can contain the following keys:

    ``db_field_name``
        A ``str`` representing the field name within the database. This only
        needs to be set when it does not match with the source field name.

    """

    extra_fields = (
        {
            'field_name': 'batch_source_name',
            'db_data_type': 'VARCHAR(255)',
        },
        {
            'field_name': 'batch_source_path',
            'db_data_type': 'VARCHAR(255)',
        },
        {
            'field_name': 'batch_import_timestamp',
            'db_data_type': 'TIMESTAMP',
        },
    )
    """
    A ``tuple`` of ``dict`` objects to prepend fields to the data.

    This list enables the possibility to add extra data to the database, for
    example data related to the import (source name, source identifier, import
    timestamp, ...).

    Each ``dict`` must contain the following keys:

    ``field_name``
        A ``str`` representing the DB field name.

    ``db_data_type``
        A ``str`` representing the field type in the database,
        eg: ``'varchar(10)'``.

    Then, for every field, you should define a method within your class
    which is is named following this template:
    ``get_extra_{field_name}_data``. This method will be called for every
    imported record with a ``dict`` containing the row data.

    .. warning:: Make sure there is no collision between these fields and the
        fields defined in :py:attr:`~.BaseImporter.mapping_list`.

    """

    _batch_import_timestamp = None

    def __init__(
            self, reader_obj, dsn, schema_name, batch_source_path, **kwargs):
        self._reader_obj = reader_obj
        self._dsn = dsn
        self._schema_name = schema_name
        self._kwargs = kwargs
        self._kwargs.update({
            'batch_source_path': batch_source_path,
        })
        logger.debug('{0} initialized'.format(self.__class__.__name__))

    def _get_vertica_batch(self):
        """
        Return a VerticaBatch instance.

        :return:
            Instance of :py:class:`.VerticaBatch`.

        """
        logger.info('Setup VerticaBatch for {0}'.format(
                self.__class__.__name__))
        return VerticaBatch(
            dsn=self._dsn,
            table_name='{0}.{1}'.format(self._schema_name, self.table_name),
            column_list=self._get_db_column_list(),
        )

    def _get_db_column_list(self):
        """
        Return a list of DB column names.

        First this will take the field names from
        :py:attr:`~.BaseImporter.extra_fields`. Then it will take the fields
        defined in :py:attr:`~.BaseImporter.mapping_list`.

        :return:
            A ``list`` of ``str`` objects.

        """
        extra_field_list = [x['field_name'] for x in self.extra_fields]
        data_field_list = [x.get('db_field_name', x['field_name'])
            for x in self.mapping_list]

        db_column_list = extra_field_list + data_field_list

        logger.info('DB columns for {0}: {1}'.format(
            self.__class__.__name__, db_column_list))
        return db_column_list

    def _get_row_value_list(self, row_data_dict):
        """
        Get list of row values which can be inserted into the DB.

        :param row_data_dict:
            A ``dict`` containing the fields and their values for one data-row.

        :return:
            A ``list`` of values, in the right column order (as generated
            by :py:meth:`~.BaseImporter._get_db_column_list`).

        """
        output_list = []
        for field_dict in self.extra_fields:
            data_method = getattr(self, 'get_extra_{0}_data'.format(
                field_dict['field_name']))
            output_list.append(data_method(row_data_dict))

        output_list.extend([row_data_dict[x['field_name']]
            for x in self.mapping_list])

        return output_list

    def _insert_into_history(self, db_cursor):
        """
        Insert import instance into batch history.

        This is used to determine the next time which objects are and aren't
        imported.

        :param db_cursor:
            An instance of :py:class:`!pyodbc.Cursor`.

        """
        db_cursor.execute(
            'INSERT INTO {batch_history_table} (batch_source_name, '
            'batch_source_type_name, batch_source_path, '
            'batch_import_timestamp) VALUES (?, ?, ?, ?)'.format(
                batch_history_table=self.batch_history_table
            ),
            self.batch_source_name,
            self.batch_source_type_name,
            self.get_extra_batch_source_path_data(None),
            self.get_extra_batch_import_timestamp_data(None),
        )

    def get_sql_create_table_statement(self):
        """
        Return SQL statement for creating the DB table.

        This will first use the fields specified in
        :py:attr:`~.BaseImporter.extra_fields`, followed by the fields in
        :py:attr:`~.BaseImporter.mapping_list`.

        :return:
            A ``str`` representing the SQL statement.

        """
        db_field_list = []

        for field_dict in self.extra_fields:
            db_field_list.append('{field_name} {db_data_type}'.format(
                **field_dict))

        for field_dict in self.mapping_list:
            db_field_name = field_dict.get(
                'db_field_name', field_dict['field_name'])
            db_field_list.append('{0} {1}'.format(
                db_field_name, field_dict['db_data_type']))

        return 'CREATE TABLE {schema}.{table_name} ({fields})'.format(
            schema=self._schema_name,
            table_name=self.table_name,
            fields=', '.join(db_field_list)
        )

    def start_import(self):
        """
        Start the import.

        This will import all the data from the ``reader_obj`` argument
        (given when constructing :py:class:`.BaseImporter`). In case there
        are no errors, it will commit the import at the end.

        :raises:
            :py:exc:`.BatchImportError` when there are errors during the
            import. Errors are logged to the logger object.

        :raises:
            :py:exc:`.AlreadyImportedError` when there already an import exists
            with the same batch-source path. Before starting your import, you
            can test this by
            calling :py:meth:`~.BaseImporter.get_batch_source_path_exists`.

        """
        batch_source_path_exists = self.get_batch_source_path_exists(
            self._dsn, self._kwargs['batch_source_path'])

        if batch_source_path_exists:
            raise AlreadyImportedError(
                'There is already an import with '
                'batch_source_path={0}'.format(
                    self._kwargs['batch_source_path']
                )
            )

        batch_obj = self._get_vertica_batch()

        for data_dict in self._reader_obj:
            batch_obj.insert_list(self._get_row_value_list(data_dict))

        logger.info('Last line inserted')

        errors_bool, errors_file_obj = batch_obj.get_errors()
        if not errors_bool:
            batch_db_cursor = batch_obj.get_cursor()
            self._insert_into_history(batch_db_cursor)
            batch_obj.commit()
        else:
            for error_line in errors_file_obj:
                logger.error('Batch error ({0}): {1}'.format(
                    self._kwargs['batch_source_path'],
                    error_line.rstrip('\r\n')))

            raise BatchImportError(
                'Errors detected during the import of '
                'batch_source_path={0}'.format(
                    self._kwargs['batch_source_path']
                )
            )

    @classmethod
    def get_batch_source_path_exists(cls, dsn, batch_source_path):
        """
        Check if the batch source-path exists in the database.

        :param dsn:
            The ODBC data source-name (``str``).

        :param batch_source_path:
            The batch source-path (``str``).

        :return:
            ``True`` if it already exists, else ``False``.

        """
        connection = get_connection(dsn)
        cursor = connection.cursor()
        cursor.execute(
            'SELECT batch_source_path FROM {batch_history_table} '
            'WHERE batch_source_name = ? AND batch_source_type_name = ? AND '
            'batch_source_path = ? LIMIT 1'.format(
                batch_history_table=cls.batch_history_table
            ),
            cls.batch_source_name,
            cls.batch_source_type_name,
            batch_source_path
        )
        row = cursor.fetchone()

        if row:
            return True
        return False

    @classmethod
    def get_last_imported_batch_source_path(cls, dsn):
        """
        Return the last imported batch source-path.

        :param dsn:
            The ODBC data source-name (``str``).

        :return:
            A ``str`` representing the last imported batch source-path.

        """
        connection = get_connection(dsn)
        cursor = connection.cursor()
        cursor.execute(
            'SELECT batch_source_path FROM {batch_history_table} '
            'WHERE batch_source_name = ? AND batch_source_type_name = ? '
            'ORDER BY batch_import_timestamp DESC LIMIT 1'.format(
                batch_history_table=cls.batch_history_table
            ),
            cls.batch_source_name,
            cls.batch_source_type_name
        )
        row = cursor.fetchone()

        if row:
            return row[0]
        return None

    def get_extra_batch_source_name_data(self, row_data_dict):
        """
        Return batch source name.

        :param row_data_dict:
            A ``dict`` containing the row-data.

        :return:
            The value set in :py:attr:`~.BaseImporter.batch_source_name`.

        """
        return self.batch_source_name

    def get_extra_batch_source_path_data(self, row_data_dict):
        """
        Return batch source path.

        :param row_data_dict:
            A ``dict`` containing the row-data.

        :return:
            A ``str`` containing the batch-source path (this is given as the
            ``batch_source_path`` argument on constructing this class).

        """
        return self._kwargs['batch_source_path']

    def get_extra_batch_import_timestamp_data(self, row_data_dict):
        """
        Return batch import timestamp.

        :param row_data_dict:
            A ``dict`` containing the row-data.

        :return:
            A ``str`` in ISO 8601 format, with a space a separator.

        """
        if not self._batch_import_timestamp:
            self._batch_import_timestamp = datetime.utcnow().isoformat(' ')
        return self._batch_import_timestamp
