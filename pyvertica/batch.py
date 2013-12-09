import codecs
import copy
import logging
import os
import tempfile
import threading
import taskthread
from Queue import Queue
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


class Query(object):
    """
    An object that executes the ``COPY`` query for batch loading.

    :param cursor:
        A :py:mod:`!pyodbc` cursor object.

    :param sql_query_str:
        A ``str`` representing the ``COPY`` query.

    :param semaphore_obj:
        An instance of :py:class:`!threading.Semaphore`.

    :param fifo_path:
        A ``str`` representing the path of the fifo file.

    :param exc_queue:
        An instance of class:`Queue.Queue` instance to put exceptions in.

    """

    daemon = True
    """
    If the script is interrupted (^C) it will kill all threads marked as daemon
    if only those are left.
    """

    def __init__(
            self, cursor, sql_query_str, fifo_path, exc_queue):
        super(Query, self).__init__()
        self.cursor = cursor
        self.sql_query_str = sql_query_str
        self.exc_queue = exc_queue
        self.fifo_path = fifo_path

    def run_query(self):
        """
        Handle executing the SQL query.

        This method is intended to be called on a task_thread.

        """
        logger.debug('Query started with SQL statement: {0}'.format(
            self.sql_query_str))
        try:
            self.cursor.execute(self.sql_query_str)
        except Exception as e:
            logger.exception('Something unexpected happened')

            # the exception will be re-raised in the main thread
            self.exc_queue.put(e)

            # we need to consume the fifo, to make sure it isn't blocking the
            # write (and thus hanging forever).
            for line in codecs.open(self.fifo_path, 'r', 'utf-8'):
                pass

        logger.debug('Query done')


class VerticaBatch(object):
    """
    Object for writing multiple records to Vertica in a batch.

    Usage example::

        from pyvertica.batch import VerticaBatch

        batch = VerticaBatch(
            odbc_kwargs={'dsn': 'VerticaDWH'},
            table_name='schema.my_table',
            truncate=True,
            column_list=['column_1', 'column_2'],
            copy_options={
                'DELIMITER': ',',
            },
            multi_batch=False
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
        :py:meth:`~.VerticaBatch.get_errors` will reflect the new series of
        inserts and thus not contain the "old" inserts.

    .. note:: Creating a new batch object will not create a lock on the target
        table. This will happen only after first insert.

    .. note:: If a batch is created with ``multi_batch = True``,
        :py:meth:`~.VerticaBatch.close_batch` must be explicity called when
        the batch resources should be closed. If ``multi_batch`` is set to
        ``False``, :py:meth:`~.VerticaBatch.close_batch` need not be called.
        In this case, while the batch is reusable, the system resources will
        be realoccated uppon each :py:meth:`~.VerticaBatch.commit`, which
        may not be desirable.

    :param table_name:
        A ``str`` representing the table name (including the schema) to write
        to. Example: ``'staging.my_table'``.

    :param odbc_kwargs:
        A ``dict`` containing the ODBC connection keyword arguments. E.g.::

            {
                'dsn': 'TestDSN',
            }

        .. seealso:: https://code.google.com/p/pyodbc/wiki/Module

    :param truncate_table:
        A ``bool`` indicating if the table needs truncating before first
        insert. Default: ``False``. *Optional*.

    :param reconnect:
        A ``bool`` passed to the connection object to decide if pyvertica
        should directly reconnect to a random node to bypass a load balancer.

    :param analyze_constraints:
        A ``bool`` indicating if a ``ANALYZE_CONSTRAINTS`` startement should
        be executed when getting errors. Default: ``True``. *Optional*.

    :param column_list:
        A ``list`` containing the columns that will be written. *Optional*.

    :param copy_options:
        A ``dict`` containing the keys to override. For a list of existing keys
        and their defaults, see :py:attr:`~.VerticaBatch.copy_options_dict`.
        *Optional*.

    :param connection:
        A ``pyodbc.Connection`` to use instead of opening a new connection. If
        this parameter is supplied, ``odbc_kwargs`` may not be supplied.
        Default: ``None``. *Optional*.

    :param multi_batch:
        A ``boolean`` to indicate if the batch should keep it's resources open
        after a call to commit. If you plan to only call
        :py:meth:`~.VerticaBatch.commit` one time, set this to false.
        Otherwise, setting ``multi_batch=True`` will prevent the batch from
        closing all of its resources.
        Default: ``False``. *Optional*.

    """
    copy_options_dict = {
        'DELIMITER': ';',
        'ENCLOSED BY': '"',
        'SKIP': 0,
        'NULL': '',
        'RECORD TERMINATOR': '\x01',
        'NO COMMIT': True,
        'REJECTEDFILE': __debug__,
        'REJECTMAX': 0,
    }
    """
    Default copy options for SQL query.

    .. note:: By default ``REJECTEDFILE`` is set to ``__debug__``, which is
       ``True``, unless you've set the ``PYTHONOPTIMIZE`` environment variable.

    """

    def __init__(
            self,
            table_name,
            odbc_kwargs={},
            truncate_table=False,
            reconnect=True,
            analyze_constraints=True,
            column_list=[],
            copy_options={},
            connection=None,
            multi_batch=False):

        if connection and odbc_kwargs:
            raise ValueError("May only specify one of "
                             "[connection, odbc_kwargs]")

        self._odbc_kwargs = odbc_kwargs
        self._table_name = table_name
        self._column_list = column_list
        self._analyze_constraints = analyze_constraints
        self.copy_options_dict.update(copy_options)
        self._batch_initialized = False
        self._multi_batch = multi_batch

        self._total_count = 0
        self._batch_count = 0

        self._in_batch = False

        if not connection:
            # make sure we are not logging any passwords :)
            odbc_kwargs_copy = copy.deepcopy(odbc_kwargs)
            if 'password' in odbc_kwargs_copy:
                odbc_kwargs_copy['password'] = '*****'
            logger.debug(
                'Initializing VerticaBatch with odbc_kwargs={0}, '
                'table_name={1}, '
                'column_list={2}'.format(
                    odbc_kwargs_copy, table_name, column_list))
            self._connection = get_connection(
                reconnect=reconnect, **self._odbc_kwargs)
        else:
            self._connection = connection

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

    def _initialize_batch(self):

        self._query_exc_queue = Queue()

        # create FIFO
        self._fifo_path = os.path.join(tempfile.mkdtemp(), 'fifo')
        os.mkfifo(self._fifo_path)

        # create rejected file obj
        if self.copy_options_dict['REJECTEDFILE']:
            self._rejected_file_obj = tempfile.NamedTemporaryFile(bufsize=0)
        self._query = Query(
            self._cursor,
            self._get_sql_lcopy_str(),
            self._fifo_path,
            self._query_exc_queue,
        )

        self._query_thread = taskthread.TaskThread(self._query.run_query)

        # Start the thread so run_task can be called
        self._query_thread.start()
        self._batch_initialized = True

    def _start_batch(self):
        """
        Start the batch.

        This will create the FIFO file, a temporary file for the rejected
        inserts and this will setup and start the :py:class:`.QueryThread`.

        """
        self._in_batch = True
        self._batch_count = 0
        if not self._batch_initialized:
            self._initialize_batch()

        self._query_thread.run_task()

        logger.debug('Opening FIFO')
        self._fifo_obj = codecs.open(self._fifo_path, 'w', 'utf-8')

        logger.debug('Batch started')

    @require_started_batch
    def _end_batch(self):
        """
        End the batch.

        This waits for the current query to finish, and executes
        close_batch if multi_batch is false.

        """
        ended_clean = True

        logger.debug('Closing FIFO')
        # The Query task will stop when there is nothing writing to
        # the fifo. This should force the current task to end.
        self._fifo_obj.close()

        logger.debug('Waiting for COPY Query to finish')
        if not self._query_thread.join_task(2):
            logger.warn('Error shutting down task thread!')
        else:
            logger.debug('Query task finished')

        if not self._multi_batch:
            ended_clean = self.close_batch() and ended_clean

        self._in_batch = False
        return ended_clean

    def close_batch(self):
        """
        Close out the batch.

        This will remove the FIFO file and stop the ``taskthread.TaskThread``.

        """

        ended_clean = True
        logger.debug('Terminating thread')
        self._query_thread.join(2)
        if self._query_thread.is_alive():
            ended_clean = False
            logging.error('Terminating thread timed out!')

        os.remove(self._fifo_path)
        os.rmdir(os.path.dirname(self._fifo_path))

        logger.debug('Batch ended')

        if not self._query_exc_queue.empty():
            raise self._query_exc_queue.get()

        self._batch_initialized = False
        return ended_clean

    def _get_num_rejected_rows(self):
        """
        Return the number of rejected rows.

        :return:
            An ``int``.

        """
        if self._in_batch:
            self._end_batch()

        if not self.get_batch_count():
            return 0

        rejected_rows = self._cursor.execute('SELECT GET_NUM_REJECTED_ROWS()')
        rejected_rows = rejected_rows.fetchone()
        return rejected_rows[0]

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
                'REJECTMAX',
                'DELIMITER',
                'ENCLOSED BY',
                'SKIP',
                'NULL',
                'RECORD TERMINATOR']:
            value = self.copy_options_dict[key]

            if isinstance(value, int):
                output_str += ' {0} {1}'.format(key, value)
            elif isinstance(value, str):
                output_str += " {0} '{1}'".format(key, value)

        # NO COMMIT statement, which needs to be at the end
        if self.copy_options_dict['NO COMMIT']:
            output_str += ' NO COMMIT'

        return output_str

    def _single_list_to_string(self,
                               value_list,
                               suffix=None):
        """
        Convert a single ``iterable`` to a string that represents one item
        in the batch.

        :param value_list:
            An ``iterable``. Each item represents one column value

        :param suffix:
            A ``string``. If specified, this character will be appended
            to the resulting string.
        """
        enclosed_by = self.copy_options_dict['ENCLOSED BY']
        escaped_enclosed_by = '\\%s' % enclosed_by
        suffix = suffix if suffix else ''
        delimiter = self.copy_options_dict['DELIMITER']

        str_value_list = (
            '%s%s%s' % (
                enclosed_by,
                unicode(value).replace(enclosed_by, escaped_enclosed_by),
                enclosed_by
            )
            if value is not None else '' for value in value_list)

        return delimiter.join(str_value_list) + suffix

    def insert_list(self, value_list):
        """
        Insert a ``list`` of values (instead of a ``str`` representing a line).

        Example::

            batch.insert_list(['value_1', 'value_2'])

        :param value_list:
            A ``list``. Each item should represent a column value.

        """
        return self.insert_line(self._single_list_to_string(value_list))

    @require_started_batch
    def insert_lists(self, value_lists, row_count=1):
        """
        Insert an ``iterable`` of ``iterable`` values (instead of a single
        string). The iterables can be lists, generators, etc.

        Example::

            batch.insert_lists([['key1', 'value1'], ['key2', 'value2']))

        :param value_lists:
            An ``iterable``. Each iterable is another ``iterable`` containing
            the values to insert.

        :param row_count:
            An ``int``. The number of rows being inserted. Since the
            ``value_lists`` parameter may be a generator, the number of
            rows is not easily determinable. Therefore, the number of
            rows being inserted must be specified.
        """
        suffix = self.copy_options_dict['RECORD TERMINATOR']
        strings = (self._single_list_to_string(value_list,
                                               suffix=suffix)
                   for value_list in value_lists)
        self._fifo_obj.write(
            "".join(strings)
        )
        self._total_count += row_count
        self._batch_count += row_count

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
            A ``tuple`` with as first item a ``int`` representing the number
            of errors. The second item is a file-like object containing the
            error-data in plain text. Since this is an instance
            of :py:class:`!tempfile.TemporaryFile`, it will be removed
            automatically.

            .. note:: The file-like object can be empty, when ``REJECTEDFILE``
               is set to ``False``.

        """
        if self._in_batch:
            self._end_batch()

        error_file_obj = tempfile.TemporaryFile(bufsize=0)

        if not self.get_batch_count():
            return(False, error_file_obj)

        error_count = self._get_num_rejected_rows()

        if self._analyze_constraints:
            try:
                analyze_constraints = self._cursor.execute(
                    "SELECT ANALYZE_CONSTRAINTS('{0}')".format(
                        self._table_name))
            except Exception as e:
                if not 'no constraints defined' in str(e).lower():
                    raise e
                analyze_constraints = None

            if analyze_constraints and analyze_constraints.rowcount > 0:
                error_count += analyze_constraints.rowcount
                error_file_obj.write(
                    'At least one constraint not met: {0}\n'.format(
                        ', '.join(analyze_constraints.fetchone())))

        if self.copy_options_dict['REJECTEDFILE']:
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

            error_file_obj.seek(0)

        return (error_count, error_file_obj)

    def commit(self):
        """
        Commit the current transaction.
        """
        batch_count = self.get_batch_count()

        if self._in_batch:
            self._end_batch()

        self._connection.commit()
        logger.info('Transaction committed, {0} lines inserted'.format(
            batch_count))

    def rollback(self):
        """
        Rollback the current transaction.
        """
        if self._in_batch:
            self._end_batch()

        self._connection.rollback()
        logger.info('Transaction rolled back')

    def get_cursor(self):
        """
        Return a cursor to the database.

        This is useful when you want to add extra data within the same
        transaction of the batch import.

        :return:
            Instance of :py:class:`!pyodbc.Cursor`.

        """
        return self._connection.cursor()
