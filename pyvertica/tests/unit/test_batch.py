# -*- coding: utf-8 -*-

import os
import stat
import tempfile
from taskthread import TaskThread
import unittest2 as unittest
from Queue import Queue

from mock import Mock, patch

from pyvertica.batch import (
    Query, VerticaBatch, require_started_batch)


class RequireStartedBatchDecoratorTestCase(unittest.TestCase):
    """
    Tests for :py:func:`.require_started_batch`.
    """
    def setUp(self):
        class TestClass(object):
            _start_batch = Mock()

            def __init__(self, in_batch):
                self._in_batch = in_batch

            @require_started_batch
            def test(self, foo):
                return foo

        self.TestClass = TestClass

    def test_not_in_batch(self):
        """
        Test when ``_in_batch`` is ``False``.
        """
        test_class = self.TestClass(False)
        self.assertEqual('foo', test_class.test('foo'))
        test_class._start_batch.assert_called_once_with()

    def test_in_batch(self):
        """
        Test when ``_in_batch`` is ``True``.
        """
        test_class = self.TestClass(True)
        self.assertEqual('foo', test_class.test('foo'))
        self.assertEqual(0, test_class._start_batch.call_count)


class QueryTestCase(unittest.TestCase):
    """
    Tests for :py:class:`.QueryThread`.
    """
    def test___init__(self):
        """
        Test initialization of :py:class:`.QueryThread`.
        """
        cursor = Mock()
        sql_query_str = Mock()
        fifo_path = Mock()
        exc_queue = Mock()

        query = Query(
            cursor, sql_query_str, fifo_path, exc_queue)

        self.assertEqual(cursor, query.cursor)
        self.assertEqual(sql_query_str, query.sql_query_str)
        self.assertEqual(fifo_path, query.fifo_path)
        self.assertEqual(exc_queue, query.exc_queue)

    def test_run_query(self):
        """
        Test :py:meth:`.QueryThread.run`.
        """
        cursor = Mock()
        sql_query_str = Mock()

        query = Query(cursor, sql_query_str, Mock(), Mock())
        query.run_query()

        cursor.execute.assert_called_once_with(sql_query_str)

    def test_run_raising_exception(self):
        """
        Test :py:meth:`.QueryThread.run` raising an exception.
        """
        file_obj = tempfile.NamedTemporaryFile(bufsize=0, delete=False)
        file_obj.write('foo\nbar\n')
        file_obj.close()

        cursor = Mock()
        cursor.execute.side_effect = Exception('boom!')
        exc_queue = Queue()

        query = Query(cursor, Mock(), file_obj.name, exc_queue)
        task_thread = TaskThread(query.run_query)
        task_thread.start()
        task_thread.run_task()
        task_thread.join_task(2)
        task_thread.join(2)

        os.remove(file_obj.name)
        self.assertTrue(isinstance(exc_queue.get(), Exception))


class VerticaBatchTestCase(unittest.TestCase):
    """
    Test for :py:class:`.VerticaBatch`.
    """
    def get_batch(self, **kwargs):
        arguments = {
            'odbc_kwargs': {'dsn': 'TestDSN'},
            'table_name': 'schema.test_table',
            'truncate_table': False,
            'column_list': [
                'column_1',
                'column_2',
                'column_3',
            ],
            'copy_options': {'DELIMITER': ',', 'SKIP': 1},
        }
        arguments.update(kwargs)
        return VerticaBatch(**arguments)

    @patch('pyvertica.batch.VerticaBatch._truncate_table')
    @patch('pyvertica.batch.get_connection')
    def test___init__no_truncate(self, get_connection, truncate_table):
        """
        Test initialization of :py:class:`.VerticaBatch` without truncate.
        """
        batch = self.get_batch()

        # variables
        self.assertEqual({'dsn': 'TestDSN'}, batch._odbc_kwargs)
        self.assertEqual('schema.test_table', batch._table_name)
        self.assertEqual(
            ['column_1', 'column_2', 'column_3'], batch._column_list)
        self.assertEqual({
            'DELIMITER': ',',
            'ENCLOSED BY': '"',
            'SKIP': 1,
            'RECORD TERMINATOR': '\x01',
            'NULL': '',
            'NO COMMIT': True,
            'REJECTEDFILE': __debug__,
            'REJECTMAX': 0,
        }, batch.copy_options_dict)
        self.assertEqual(0, batch._total_count)
        self.assertEqual(0, batch._batch_count)
        self.assertFalse(batch._in_batch)

        # db connection
        get_connection.assert_called_once_with(dsn='TestDSN', reconnect=True)
        self.assertEqual(get_connection.return_value, batch._connection)
        batch._connection.cursor.assert_called_once_with()
        self.assertEqual(batch._connection.cursor.return_value, batch._cursor)

        # truncate
        self.assertEqual(0, truncate_table.call_count)

    @patch('pyvertica.batch.VerticaBatch._truncate_table')
    @patch('pyvertica.batch.get_connection')
    def test__init__truncate(self, get_connection, truncate_table):
        """
        Test truncate during initialization of :py:class:`.VerticaBatch`.
        """
        self.get_batch(truncate_table=True)
        truncate_table.assert_called_once_with()

    def test__init__connection(self):
        """"
        Test passing connection to VerticaBatch instead of odbc_kwargs.
        """
        connection = Mock()
        batch = VerticaBatch(table_name='table',
                             column_list=[],
                             connection=connection)
        self.assertEqual(connection, batch._connection)
        connection.cursor.assert_any_call()

    def test__init__connection_or_odbc_kwargs(self):
        args = {
            'odbc_kwargs': {'dsn': 'TestDSN'},
            'connection': Mock()
        }
        try:
            VerticaBatch(table_name='table',
                         column_list=[],
                         odbc_kwargs=args,
                         connection="connection")
            self.fail("Should throw ValueError")
        except ValueError:
            pass

    @patch('pyvertica.batch.get_connection')
    def test_truncate_table(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch._truncate_table`.
        """
        batch = self.get_batch()
        batch._cursor = Mock()

        batch._truncate_table()
        batch._cursor.execute.assert_called_once_with(
            'TRUNCATE TABLE schema.test_table')

    @patch('taskthread.TaskThread')
    @patch('pyvertica.batch.codecs')
    @patch('pyvertica.batch.VerticaBatch._get_sql_lcopy_str')
    @patch('pyvertica.batch.Query')
    @patch('pyvertica.batch.get_connection')
    def test__start_batch(self,
                          get_connection,
                          QueryMock,
                          get_sql_lcopy_str,
                          codecs,
                          TaskThreadMock):
        """
        Test :py:meth:`.VerticaBatch._start_batch`.
        """
        thread = TaskThreadMock.return_value
        query = QueryMock.return_value
        query.run_query = Mock()
        batch = self.get_batch()
        batch._batch_count = 10

        batch._start_batch()

        self.assertTrue(batch._in_batch)
        self.assertEqual(0, batch._batch_count)

        # test files
        self.assertTrue(stat.S_ISFIFO(os.stat(batch._fifo_path).st_mode))
        self.assertTrue(os.path.exists(batch._rejected_file_obj.name))
        codecs.open.assert_called_once_with(batch._fifo_path, 'w', 'utf-8')
        self.assertEqual(codecs.open.return_value, batch._fifo_obj)
        self.assertTrue(batch._batch_initialized)

        # test thread setup
        self.assertEqual(thread, batch._query_thread)
        thread.start.assert_called_once_with()

        QueryMock.assert_called_once_with(
            batch._cursor,
            batch._get_sql_lcopy_str.return_value,
            batch._fifo_path,
            batch._query_exc_queue,
        )
        self.assertEqual(
            QueryMock.return_value,
            batch._query
        )

    @patch('pyvertica.batch.os.remove')
    @patch('pyvertica.batch.os.rmdir')
    @patch('pyvertica.batch.get_connection')
    @patch('pyvertica.batch.VerticaBatch._start_batch')
    def test__end_batch_clean(
            self, start_batch, get_connection, remove, rmdir):
        """
        Test :py:meth:`.VerticaBatch._end_batch` ending clean.

        :py:mod:`!time` is mocked to speedup the tests since there is no need
        to actually wait.

        """
        query_thread = Mock()
        query_thread.is_alive.return_value = False

        batch = self.get_batch()
        batch._fifo_path = '/tmp/abcd1234/fifo'
        batch._fifo_obj = Mock()
        batch._query_thread = query_thread
        batch._query_exc_queue = Mock()
        batch._query_exc_queue.empty.return_value = True

        end_return = batch._end_batch()

        batch._fifo_obj.close.assert_called_once_with()

        query_thread.join.assert_called_once_with(2)
        query_thread.is_alive.assert_called_once_with()

        os.remove('/tmp/abcd1234/fifo')
        os.rmdir('/tmp/abcd1234')

        self.assertFalse(batch._in_batch)
        self.assertTrue(end_return)

    @patch('pyvertica.batch.os.remove')
    @patch('pyvertica.batch.os.rmdir')
    @patch('pyvertica.batch.VerticaBatch._start_batch')
    @patch('pyvertica.batch.get_connection')
    def test__end_batch_dirty(
            self, get_connection, start_batch, rmdir, remove):
        """
        Test :py:meth:`.VerticaBatch._end_batch` ending 'dirty'.
        """
        cursor = get_connection.return_value.cursor.return_value
        cursor.execute.return_value.fetchone.return_value = 1

        query_thread = Mock()
        query_thread.is_alive.return_value = True

        batch = self.get_batch()
        batch._fifo_path = '/tmp/abcd1234/fifo'
        batch._fifo_obj = Mock()
        batch._query_thread = query_thread
        batch._query_exc_queue = Mock()
        batch._query_exc_queue.empty.return_value = True

        os.remove('/tmp/abcd1234/fifo')
        os.rmdir('/tmp/abcd1234')

        self.assertFalse(batch._end_batch())
        self.assertFalse(batch._in_batch)

    @patch('pyvertica.batch.get_connection')
    def test_get_batch_count(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.get_batch_count`.
        """
        batch = self.get_batch()
        batch._batch_count = Mock()

        self.assertEqual(batch._batch_count, batch.get_batch_count())

    @patch('pyvertica.batch.get_connection')
    def test_get_total_count(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.get_total_count`.
        """
        batch = self.get_batch()
        batch._total_count = Mock()

        self.assertEqual(batch._total_count, batch.get_total_count())

    @patch('pyvertica.batch.VerticaBatch._start_batch')
    @patch('pyvertica.batch.get_connection')
    def test__get_sql_lcopy_str(self, get_connection, start_batch):
        """
        Test :py:meth:`.VerticaBatch._get_sql_lcopy_str`.
        """
        batch = self.get_batch()
        batch._fifo_path = '/tmp/fifo'
        batch._rejected_file_obj = Mock()
        batch._rejected_file_obj.name = '/tmp/rejected'

        self.assertEqual(
            "COPY schema.test_table (column_1, column_2, column_3) "
            "FROM LOCAL '/tmp/fifo' REJECTED DATA '/tmp/rejected' "
            "REJECTMAX 0 "
            "DELIMITER ',' ENCLOSED BY '\"' SKIP 1 NULL '' "
            "RECORD TERMINATOR '\x01' NO COMMIT",
            batch._get_sql_lcopy_str()
        )

    @patch('pyvertica.batch.get_connection', Mock())
    def test__get_num_rejected_rows(self):
        """
        Test :py:meth:`.VerticaBatch._get_num_rejected_rows`.
        """
        batch = self.get_batch()
        batch._in_batch = False
        batch.get_batch_count = Mock(return_value=1)
        batch._cursor = Mock()

        result = batch._cursor.execute.return_value
        result.fetchone.return_value = [10]

        self.assertEqual(10, batch._get_num_rejected_rows())

    @patch('pyvertica.batch.get_connection', Mock())
    def test__single_list_to_string(self):
        """
        Test :py:meth:`.VerticaBatch._single_list_to_string`.
        """
        batch = self.get_batch()
        single_list = ['val1', 'val2']
        expected = u'"val1","val2"'
        self.assertEqual(expected,
                         batch._single_list_to_string(single_list))

    @patch('pyvertica.batch.get_connection', Mock())
    def test__single_list_to_string_suffix(self):
        """
        Test :py:meth:`.VerticaBatch._single_list_to_string`.
        """
        batch = self.get_batch()
        single_list = ['val1', 'val2']
        expected = u'"val1","val2"SUFFIX'
        self.assertEqual(expected,
                         batch._single_list_to_string(single_list,
                                                      suffix='SUFFIX'))

    @patch('pyvertica.batch.get_connection')
    def test_insert_list(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.insert_list`.
        """
        batch = self.get_batch()
        batch.insert_line = Mock()

        self.assertEqual(
            batch.insert_line.return_value,
            batch.insert_list(
                [u'valué1', 'valu"e2', 'None', None, 100, 'value4'])
        )
        batch.insert_line.assert_called_once_with(
            u'"valué1","valu\\"e2","None",,"100","value4"')

    @patch('pyvertica.batch.VerticaBatch._start_batch')
    @patch('pyvertica.batch.get_connection')
    def test_insert_lists(self, get_connection, start_batch):
        """
        Test :py:meth:`.VerticaBatch.insert_lists`.
        """
        batch = self.get_batch()
        batch._fifo_obj = Mock()

        lists = [
            ['line1value1', "line1value2"],
            ['line2value1', "line2value2"]
        ]

        batch.insert_lists(lists, row_count=2)
        batch._fifo_obj.write.assert_called_once_with(
            u'"line1value1","line1value2"\x01"line2value1","line2value2"\x01')
        self.assertEqual(2, batch._total_count)
        self.assertEqual(2, batch._batch_count)

    @patch('pyvertica.batch.VerticaBatch._start_batch')
    @patch('pyvertica.batch.get_connection')
    def test_insert_line(self, get_connection, start_batch):
        """
        Test :py:meth:`.VerticaBatch.insert_line`.
        """
        batch = self.get_batch()
        batch._fifo_obj = Mock()

        batch.insert_line('"value1";"value2";"value3"')

        batch._fifo_obj.write.assert_called_once_with(
            '"value1";"value2";"value3"\x01')
        self.assertEqual(1, batch._total_count)
        self.assertEqual(1, batch._batch_count)

    @patch('pyvertica.batch.VerticaBatch._start_batch')
    @patch('pyvertica.batch.get_connection')
    def test_insert_raw(self, get_connection, start_batch):
        """
        Test :py:meth:`.VerticaBatch.insert_raw`.
        """
        batch = self.get_batch()
        batch._fifo_obj = Mock()

        raw_mock = Mock()

        batch.insert_raw(raw_mock)
        batch._fifo_obj.write.assert_called_with(raw_mock)
        self.assertEqual(1, batch._total_count)
        self.assertEqual(1, batch._batch_count)

    @patch('pyvertica.batch.get_connection')
    def test_get_errors_exception(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.get_errors` raising exception.
        """
        batch = self.get_batch()
        batch.get_batch_count = Mock(return_value=10)
        batch._in_batch = True
        batch._end_batch = Mock()
        batch._cursor = Mock()
        batch._get_num_rejected_rows = Mock(return_value=0)

        batch._cursor.execute.side_effect = Exception('Kaboom!')

        self.assertRaises(Exception, batch.get_errors)
        batch._end_batch.assert_called_once_with()

    @patch('pyvertica.batch.get_connection')
    def test_get_errors_no_constraints(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.get_errors` without constraints.
        """
        batch = self.get_batch()
        batch.get_batch_count = Mock(return_value=10)
        batch._end_batch = Mock()
        batch._cursor = Mock()
        batch._rejected_file_obj = tempfile.NamedTemporaryFile()
        batch._get_num_rejected_rows = Mock(return_value=0)

        batch._cursor.execute.side_effect = Exception(
            'Somewhere are no constraints defined, oh dear!')

        self.assertFalse(batch.get_errors()[0])
        self.assertEqual(0, batch._end_batch.call_count)

    @patch('pyvertica.batch.get_connection')
    def test_get_errors_disabled_analyze_constraints(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.get_errors` when analyze_contraints=False.
        """
        batch = self.get_batch(analyze_constraints=False)
        batch.get_batch_count = Mock(return_value=10)
        batch._end_batch = Mock()
        batch._cursor = Mock()
        batch._rejected_file_obj = tempfile.NamedTemporaryFile()

        self.assertEqual(0, batch._cursor.execute.call_count)

    @patch('pyvertica.batch.get_connection')
    def test_get_errors_constraint_error(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.get_errors` with constraint errors.
        """
        batch = self.get_batch()
        batch.get_batch_count = Mock(return_value=10)
        batch._end_batch = Mock()
        batch._cursor = Mock()
        batch._rejected_file_obj = tempfile.NamedTemporaryFile()
        batch._get_num_rejected_rows = Mock(return_value=0)

        batch._cursor.execute.return_value.rowcount = 10
        batch._cursor.execute.return_value.fetchone.return_value = ['a', 'b']

        errors_tuple = batch.get_errors()

        self.assertEqual(10, errors_tuple[0])
        self.assertEqual(
            'At least one constraint not met: a, b\n', errors_tuple[1].read())

    @patch('pyvertica.batch.get_connection')
    def test_get_errors_rejected(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.get_errors` with rejected errors.
        """
        batch = self.get_batch()
        batch.get_batch_count = Mock(return_value=10)
        batch._end_batch = Mock()
        batch._cursor = Mock()
        batch._rejected_file_obj = tempfile.NamedTemporaryFile()
        batch._get_num_rejected_rows = Mock(return_value=123)

        batch._cursor.execute.return_value.rowcount = 0

        with open(batch._rejected_file_obj.name, 'w') as f:
            f.write('123\x01456\x01')

        errors_tuple = batch.get_errors()

        self.assertEqual(123, errors_tuple[0])
        self.assertEqual(
            'Rejected data at line: 123\nRejected data at line: 456\n',
            errors_tuple[1].read()
        )

    @patch('pyvertica.batch.tempfile')
    @patch('pyvertica.batch.get_connection')
    def test_get_errors_no_batch_count(self, get_connection, tempfile):
        """
        Test :py:meth:`.VerticaBatch.get_errors` without batch count.
        """
        batch = self.get_batch()
        batch.get_batch_count = Mock(return_value=0)
        batch._end_batch = Mock()

        self.assertEqual(
            (False, tempfile.TemporaryFile.return_value),
            batch.get_errors()
        )

    @patch('pyvertica.batch.get_connection')
    def test_commit_in_batch(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.commit` while in batch.
        """
        batch = self.get_batch()
        batch._in_batch = True
        batch._end_batch = Mock()
        batch._connection = Mock()

        batch.commit()

        batch._end_batch.assert_called_once_with()
        batch._connection.commit.assert_called_with()

    @patch('pyvertica.batch.get_connection')
    def test_commit_not_in_batch(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.commit` while not in batch.
        """
        batch = self.get_batch()
        batch._end_batch = Mock()
        batch._connection = Mock()

        batch.commit()

        self.assertEqual(0, batch._end_batch.call_count)
        batch._connection.commit.assert_called_once_with()

    @patch('pyvertica.batch.get_connection')
    def test_rollback_in_batch(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.rollback` while in batch.
        """
        batch = self.get_batch()
        batch._in_batch = True
        batch._end_batch = Mock()
        batch._connection = Mock()

        batch.rollback()

        batch._end_batch.assert_called_once_with()
        batch._connection.rollback.assert_called_once_with()

    @patch('pyvertica.batch.get_connection')
    def test_rollback_not_in_batch(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.rollback` while not in batch.
        """
        batch = self.get_batch()
        batch._in_batch = False
        batch._end_batch = Mock()
        batch._connection = Mock()

        batch.rollback()

        self.assertEqual(0, batch._end_batch.call_count)
        batch._connection.rollback.assert_called_once_with()

    @patch('pyvertica.batch.get_connection')
    def test_get_cursor(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.get_cursor`.
        """
        batch = self.get_batch()
        batch._connection = Mock()

        self.assertEqual(
            batch._connection.cursor.return_value, batch.get_cursor())
        batch._connection.cursor.assert_called_once_with()
