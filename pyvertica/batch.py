import codecs
import logging
import os
import tempfile
import threading
from functools import wraps

from pyvertica.connection import get_connection


logger = logging.getLogger(__name__)


def require_started_batch(func):
    """
    Decorator to assure that the batch has been started before calling ``func``

    This decorator should only be used on class methods. As well this assumes
    that the class has an attribute called ``_in_batch`` (``bool``) containing
    the state and a method called ``_start_batch`` (without arguments). The
    latter will be called to start the batch.

    :return:
        Decorated ``func``.

    """
    @wraps(func)
    def inner_func(self, *args, **kwargs):
        if not self._in_batch:
            self._start_batch()
        return func(self, *args, **kwargs)
    return inner_func


class QueryThread(threading.Thread):
    """
    Thread object which will execute the ``COPY`` query.

    :param cursor:
        A :py:mod:`!pyodbc` cursor object.

    :param sql_query_str:
        A ``str`` representing the ``COPY`` query.

    :param semaphore_obj:
        An instance of :py:class:`!threading.Semaphore`.

    """

    daemon = True
    """
    If the script is interrupted (^C) it will kill all threads marked as daemon
    if only those are left.
    """

    def __init__(self, cursor, sql_query_str, semaphore_obj):
        super(QueryThread, self).__init__()
        self.cursor = cursor
        self.sql_query_str = sql_query_str
        self.semaphore_obj = semaphore_obj

    def run(self):
        """
        Handle executing the SQL query.

        This method will be called when starting this thread.

        """
        logger.info('Thread started with SQL statement: {0}'.format(
            self.sql_query_str))
        self.cursor.execute(self.sql_query_str)
        logger.info('Thread done')
        self.semaphore_obj.release()


class VerticaBatch(object):
    """
    Object for writing multiple records to Vertica in a batch.

    Usage example::

        from pyvertica.batch import VerticaBatch

        batch = VerticaBatch(
            dsn='VerticaDWH',
            table_name='schema.my_table',
            truncate=True,
            column_list=['column_1', 'column_2'],
            copy_options={
                'DELIMITER': ',',
            }
        )

        row_list = [
            ['row_1_val_1', 'row_1_val_2'],
            ['row_2_val_1', 'row_2_val_2'],
            ...
        ]

        for column_data_list in row_list:
            batch.insert_list(column_data_list)

        error_bool, error_file_obj = batch.get_errors()

        if error_bool:
            print error_file_obj.read()

        batch.commit()

    .. note:: It is also possible to call :py:meth:`~.VerticaBatch.commit`
        multiple times (for example after every 50000 records). Please note
        that after the first insert and after calling
        :py:meth:`~.VerticaBatch.commit`, the output of
        :py:meth:`~.VerticaBatch.get_errors` will reflect the new serie of
        inserts and thus not contain the "old" inserts.

    .. note:: Creating a new batch object will not create a lock on the target
        table. This will happen only after first insert.

    .. note:: Although the batch object is automagically reusable, after a
        :py:meth:`~.VerticaBatch.commit` the locks are realeased up to next
        insert.


    :param dsn:
        A ``str`` representing the data source name.

    :param table_name:
        A ``str`` representing the table name (including the schema) to write
        to. Example: ``'staging.my_table'``.

    :param truncate_table:
        A ``bool`` indicating if the table needs truncating before first
        insert. Default: ``False``. *Optional*.

    :param column_list:
        A ``list`` containing the columns that will be written. *Optional*.

    :param copy_options:
        A ``dict`` containing the keys to override. For a list of existing keys
        and their defaults, see :py:attr:`~.VerticaBatch.copy_options_dict`.
        *Optional*.

    """
    copy_options_dict = {
        'DELIMITER': ';',
        'ENCLOSED BY': '"',
        'SKIP': 0,
        'NULL': '',
        'RECORD TERMINATOR': '\x01',
        'NO COMMIT': True,
        'REJECTEDFILE': True,
    }
    """
    Default copy options for SQL query.
    """

    def __init__(
                self,
                dsn,
                table_name,
                truncate_table=False,
                column_list=[],
                copy_options={},
            ):
        logger.debug(
            'Initializing VerticaBatch with dsn={0}, table_name={1}, '
            'column_list={2}'.format(dsn, table_name, column_list))

        self._dsn = dsn
        self._table_name = table_name
        self._column_list = column_list
        self.copy_options_dict.update(copy_options)

        self._total_count = 0
        self._batch_count = 0

        self._in_batch = False

        # setup db connection
        self._connection = get_connection(self._dsn)
        self._cursor = self._connection.cursor()

        # truncate table, if needed
        if truncate_table:
            self._truncate_table()

    def _truncate_table(self):
        """
        Truncate table.
        """
        logger.info('Truncating table {0}'.format(self._table_name))
        self._cursor.execute('TRUNCATE TABLE {0}'.format(self._table_name))

    def _start_batch(self):
        """
        Start the batch.

        This will create the FIFO file, a temporary file for the rejected
        inserts and this will setup and start the :py:class:`.QueryThread`.

        """
        self._in_batch = True
        self._batch_count = 0

        # create FIFO
        self._fifo_path = os.path.join(tempfile.mkdtemp(), 'fifo')
        os.mkfifo(self._fifo_path)

        # create rejected file obj
        self._rejected_file_obj = tempfile.NamedTemporaryFile(bufsize=0)

        # setup query thread
        self._query_thread_semaphore_obj = threading.Semaphore(0)
        self._query_thread = QueryThread(
            self._cursor,
            self._get_sql_lcopy_str(),
            self._query_thread_semaphore_obj,
        )
        self._query_thread.start()

        logger.debug('Opening FIFO')
        self._fifo_obj = codecs.open(self._fifo_path, 'w', 'utf-8')

        logger.info('Batch started')

    @require_started_batch
    def _end_batch(self):
        """
        End the batch.

        This will remove the FIFO file and stop the :py:class:`.QueryThread`.

        """
        ended_clean = True

        logger.debug('Closing FIFO')
        self._fifo_obj.close()

        logger.debug('Waiting for thread to finish')
        self._query_thread_semaphore_obj.acquire()
        logger.debug('Thread finished')

        logger.debug('Terminating thread')
        self._query_thread.join(2)
        if self._query_thread.is_alive():
            ended_clean = False
            logging.error('Terminating thread timed out!')

        self._in_batch = False
        os.remove(self._fifo_path)
        os.rmdir(os.path.dirname(self._fifo_path))

        logger.info('Batch ended')
        return ended_clean

    def get_batch_count(self):
        """
        Return number (``int``) of inserted items since last commit.

        .. warning:: When using :py:meth:`~.VerticaBatch.insert_raw` this
            value represents the number of raw ``str`` objects inserted, not
            the number of lines!

        :return:
            An ``int``.

        """
        return self._batch_count

    def get_total_count(self):
        """
        Return total number (``int``) of inserted items.

        .. warning:: When using :py:meth:`~.VerticaBatch.insert_raw` this
            value represents the number of raw ``str`` objects inserted, not
            the number of lines!

        :return:
            An ``int``.

        """
        return self._total_count

    @require_started_batch
    def _get_sql_lcopy_str(self):
        """
        Get ``str`` representing the COPY query.

        :return:
            A ``str`` representing the query.

        """
        # table name
        output_str = 'COPY {0}'.format(self._table_name)

        # columns, if available
        if self._column_list:
            output_str += ' ({0})'.format(', '.join(self._column_list))

        # fifo path
        output_str += " FROM LOCAL '{0}'".format(self._fifo_path)

        # rejected file
        if self.copy_options_dict['REJECTEDFILE']:
            output_str += " REJECTED DATA '{0}'".format(
                self._rejected_file_obj.name)

        # other arguments which map one-to-one
        for key in [
                    'DELIMITER',
                    'ENCLOSED BY',
                    'SKIP',
                    'NULL',
                    'RECORD TERMINATOR'
                ]:
            value = self.copy_options_dict[key]

            if isinstance(value, int):
                output_str += ' {0} {1}'.format(key, value)
            elif isinstance(value, str):
                output_str += " {0} '{1}'".format(key, value)

        # NO COMMIT statement, which needs to be at the end
        if self.copy_options_dict['NO COMMIT']:
            output_str += ' NO COMMIT'

        return output_str

    def insert_list(self, value_list):
        """
        Insert a ``list`` of values (instead of a ``str`` representing a line).

        Example::

            batch.insert_list(['value_1', 'value_2'])

        :param value_list:
            A ``list``. Each item should represent a column value.

        """
        enclosed_by = self.copy_options_dict['ENCLOSED BY']
        escaped_enclosed_by = '\\%s' % enclosed_by

        str_value_list = [
            '%s%s%s' % (
                enclosed_by,
                unicode(value).replace(enclosed_by, escaped_enclosed_by),
                enclosed_by
            )
            if value is not None
            else '' for value in value_list]

        insert_str = self.copy_options_dict['DELIMITER'].join(str_value_list)

        return self.insert_line(insert_str)

    @require_started_batch
    def insert_line(self, line_str):
        """
        Insert a ``str`` containing all the values.

        This is useful when inserting lines directly from a CSV file for
        example.

        .. note:: When you have a loghandler with ``DEBUG`` level, every query
            will be logged. For performance reason, this log statement is only
            executed when ``__debug__`` equals ``True`` (which is the default
            case). For a better performance, you should invoke the Python
            interpreter with the ``-O`` argument or set the environment
            variable ``PYTHONOPTIMIZE`` to something.

        Example::

            batch.insert_line('"value_1";"value_2"')

        :param line_str:
            A ``str`` representing the line to insert. Make sure the ``str``
            is formatted according :py:attr:`~.VerticaBatch.copy_options_dict`.
            Example: ``'"value1";"value2";"value3"'``.

        """
        if __debug__:
            logger.debug(u'Inserting line: {0}'.format(line_str))

        self._fifo_obj.write(
            line_str + self.copy_options_dict['RECORD TERMINATOR'])

        self._total_count += 1
        self._batch_count += 1

    @require_started_batch
    def insert_raw(self, raw_str):
        """
        Insert a raw ``str``.

        A raw ``str`` does not have to be a complete row, but can be a part of
        a row or even multiple rows. This is useful when you have a file that
        is already in a format readable by Vertica.
        """
        if __debug__:
            logger.debug(u'Inserting raw: {0}'.format(raw_str))

        self._fifo_obj.write(raw_str)

        self._total_count += 1
        self._batch_count += 1

    def get_errors(self):
        """
        Get errors that were raised since the last commit.

        This will check constraint errors and rejected data by the database.
        Please note that this will remove the rejected data file after calling
        this method. Therfore it is not possible to call this method more than
        once per batch!

        .. note:: Since this is checking the contraints as well, it is assumed
            that all contrains were met before starting the batch. Otherwise,
            these errors will show up within this method.

        :return:
            A ``tuple`` with as first item a ``bool`` representing if there are
            errors (``True`` = errors, ``False`` = no errors). The second item
            is a file-like object containing the error-data in plain text.
            Since this is an instance of :py:class:`!tempfile.TemporaryFile`,
            it will be removed automatically.

        """
        if self._in_batch:
            self._end_batch()

        error_file_obj = tempfile.TemporaryFile(bufsize=0)

        if not self.get_batch_count():
            return(False, error_file_obj)

        try:
            analyze_constraints = self._cursor.execute(
                "SELECT ANALYZE_CONSTRAINTS('{0}')".format(self._table_name))
        except Exception as e:
            if not 'no constraints defined' in str(e).lower():
                raise e
            analyze_constraints = None

        if analyze_constraints and analyze_constraints.rowcount > 0:
            error_file_obj.write(
                'At least one constraint not met: {0}\n'.format(
                    ', '.join(analyze_constraints.fetchone())))

        self._rejected_file_obj.seek(0)
        file_size = os.path.getsize(self._rejected_file_obj.name)
        read_func = lambda: self._rejected_file_obj.read(1024 * 1024)
        error_prefix = 'Rejected data at line: '

        for counter, line in enumerate(iter((read_func), '')):
            if counter == 0:
                error_file_obj.write(error_prefix)

            line = line.replace(
                self.copy_options_dict['RECORD TERMINATOR'],
                '\n{0}'.format(error_prefix)
            )

            if self._rejected_file_obj.tell() == file_size:
                line = line[:-len(error_prefix)]

            error_file_obj.write(line)

        errors = error_file_obj.tell() > 0
        error_file_obj.seek(0)

        return (errors, error_file_obj)

    def commit(self):
        """
        Commit the current transaction.
        """
        batch_count = self.get_batch_count()

        if self._in_batch:
            self._end_batch()

        self._connection.commit()
        logger.info('Transaction comitted, {0} lines inserted'.format(
            batch_count))

    def get_cursor(self):
        """
        Return a cursor to the database.

        This is useful when you want to add extra data within the same
        transaction of the batch import.

        :return:
            Instance of :py:class:`!pyodbc.Cursor`.

        """
        return self._connection.cursor()
