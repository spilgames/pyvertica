# -*- coding: utf-8 -*-

import os
import stat
import tempfile
import unittest2 as unittest

from mock import Mock, patch

from pyvertica.batch import (
    QueryThread, VerticaBatch, require_started_batch)


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


class QueryThreadTestCase(unittest.TestCase):
    """
    Tests for :py:class:`.QueryThread`.
    """
    def test___init__(self):
        """
        Test initialization of :py:class:`.QueryThread`.
        """
        cursor = Mock()
        sql_query_str = Mock()
        semaphore_obj = Mock()

        query_thread = QueryThread(cursor, sql_query_str, semaphore_obj)

        self.assertEqual(cursor, query_thread.cursor)
        self.assertEqual(sql_query_str, query_thread.sql_query_str)
        self.assertEqual(semaphore_obj, query_thread.semaphore_obj)

    def test_run(self):
        """
        Test :py:meth:`.QueryThread.run`.
        """
        cursor = Mock()
        sql_query_str = Mock()
        semaphore_obj = Mock()

        query_thread = QueryThread(cursor, sql_query_str, semaphore_obj)
        query_thread.run()

        cursor.execute.assert_called_once_with(sql_query_str)
        semaphore_obj.release.assert_called_once_with()


class VerticaBatchTestCase(unittest.TestCase):
    """
    Test for :py:class:`.VerticaBatch`.
    """
    def get_batch(self, **kwargs):
        arguments = {
            'dsn': 'TestDSN',
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
        self.assertEqual('TestDSN', batch._dsn)
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
            'REJECTEDFILE': True,
        }, batch.copy_options_dict)
        self.assertEqual(0, batch._total_count)
        self.assertEqual(0, batch._batch_count)
        self.assertFalse(batch._in_batch)

        # db connection
        get_connection.assert_called_once_with('TestDSN')
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

    @patch('pyvertica.batch.codecs')
    @patch('pyvertica.batch.VerticaBatch._get_sql_lcopy_str')
    @patch('pyvertica.batch.QueryThread')
    @patch('pyvertica.batch.threading')
    @patch('pyvertica.batch.get_connection')
    def test__start_batch(
                self,
                get_connection,
                threading,
                QueryThreadMock,
                get_sql_lcopy_str,
                codecs,
            ):
        """
        Test :py:meth:`.VerticaBatch._start_batch`.
        """
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

        # test thread setup
        threading.Semaphore.assert_called_once_with(0)
        self.assertEqual(
            threading.Semaphore.return_value,
            batch._query_thread_semaphore_obj
        )
        QueryThreadMock.assert_called_once_with(
            batch._cursor,
            batch._get_sql_lcopy_str.return_value,
            batch._query_thread_semaphore_obj,
        )
        self.assertEqual(
            QueryThreadMock.return_value,
            batch._query_thread
        )
        batch._query_thread.start.assert_called_once_with()

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
        batch._query_thread_semaphore_obj = Mock()
        batch._query_thread = query_thread

        end_return = batch._end_batch()

        batch._fifo_obj.close.assert_called_once_with()
        batch._query_thread_semaphore_obj.acquire.assert_called_once_with()

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
        batch._query_thread_semaphore_obj = Mock()
        batch._query_thread = query_thread

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
            "DELIMITER ',' ENCLOSED BY '\"' SKIP 1 NULL '' "
            "RECORD TERMINATOR '\x01' NO COMMIT",
            batch._get_sql_lcopy_str()
        )

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

        batch._cursor.execute.side_effect = Exception(
            'Somewhere are no constraints defined, oh dear!')

        self.assertFalse(batch.get_errors()[0])
        self.assertEqual(0, batch._end_batch.call_count)

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

        batch._cursor.execute.return_value.fetchone.return_value = ['a', 'b']

        errors_tuple = batch.get_errors()

        self.assertTrue(errors_tuple[0])
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

        batch._cursor.execute.return_value.rowcount = 0

        with open(batch._rejected_file_obj.name, 'w') as f:
            f.write('123\n456\n')

        errors_tuple = batch.get_errors()

        self.assertTrue(errors_tuple[0])
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
    def test_get_cursor(self, get_connection):
        """
        Test :py:meth:`.VerticaBatch.get_cursor`.
        """
        batch = self.get_batch()
        batch._connection = Mock()

        self.assertEqual(
            batch._connection.cursor.return_value, batch.get_cursor())
        batch._connection.cursor.assert_called_once_with()
