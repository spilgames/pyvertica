import unittest2 as unittest

import pyodbc

# Sidable logging during unittests
import logging
logging.disable(logging.CRITICAL)

from subprocess import CalledProcessError
from mock import Mock, call, patch
from pyvertica.migrate import VerticaMigrator, VerticaMigratorError


class VerticaMigratorConnection(unittest.TestCase):
    """
    Test for the connection steps of :py:class:`.VerticaMigrator`.
    Another class will test the rest, mocking out globally the connections
    """


class VerticaMigratorTestCase(unittest.TestCase):
    """
    Test for :py:class:`.VerticaMigrator`.
    """

    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    def get_migrator(self, sanity_checks, set_connections, **kwargs):
        migrator = VerticaMigrator('SourceDSN', 'TargetDSN', False, **kwargs)
        return migrator

    ### different sanity options.
    @patch('pyvertica.migrate.VerticaMigrator._source_ip',
           '1.2.3.4',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip',
           '5.6.7.8',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_ok(self, cnx, target):
        """
        Test :py:meth:`.VerticaMigrator._sanity_checks`.
        No exception should be raised if IP are different.
        """
        target.execute.return_value.fetchone.return_value = [0]
        VerticaMigrator('SourceDSN', 'TargetDSN', False)

    @patch('pyvertica.migrate.VerticaMigrator._source_ip',
           '1.2.3.4',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip',
           '1.2.3.4',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_same_ip_diff_db(self, cnx, target, source):
        """
        Test :py:meth:`.VerticaMigrator._sanity_checks`.
        No exception should be raised if IPs are identical but
        DBs are different.
        """
        target.execute.return_value.fetchone.side_effect = [['targetDB'], [0]]
        source.execute.return_value.fetchone.return_value = ['sourceDB']
        VerticaMigrator('SourceDSN', 'TargetDSN', False)

    @patch('pyvertica.migrate.VerticaMigrator._source_ip',
           '1.2.3.4',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip',
           '1.2.3.4',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_same_ip_same_db(self, cnx, target, source):
        """
        Test :py:meth:`.VerticaMigrator._sanity_checks`.
        Exception if IPs are identical and DBs are identical.
        """
        target.execute.return_value.fetchone.side_effect = [['targetDB'], [0]]
        source.execute.return_value.fetchone.return_value = ['targetDB']
        self.assertRaises(VerticaMigratorError, VerticaMigrator,
                          'SourceDSN', 'TargetDSN', False)

    @patch('pyvertica.migrate.VerticaMigrator._source_ip',
           '1.2.3.4',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip',
           '5.6.7.8',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_not_empty(self, cnx, target):
        """
        Test :py:meth:`.VerticaMigrator._sanity_checks`.
        Exception if target DB is not empty.
        """
        target.execute.return_value.fetchone.return_value = [42]
        self.assertRaises(VerticaMigratorError, VerticaMigrator,
                          'SourceDSN', 'TargetDSN', False)

    @patch('pyvertica.migrate.VerticaMigrator._source_ip',
           '1.2.3.4',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip',
           '5.6.7.8',
           create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_not_empty_but_thats_ok(self, cnx, target):
        """
        Test :py:meth:`.VerticaMigrator._sanity_checks`.
        No exception if target DB is not empty and we know it:
         even_not_empty=True.
        """
        target.execute.return_value.fetchone.return_value = [42]
        VerticaMigrator('SourceDSN', 'TargetDSN', False, even_not_empty=True)

    # ### get DDLs
    @patch('pyvertica.migrate.subprocess.Popen', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source_con', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    def test__get_ddls_empty(self, source, source_con, Popen):
        """
        Test :py:meth:`.VerticaMigrator._get_ddls`.
        None from DB, get from subprocess.
        """
        sql = 'CREATE TABLE cheese (id INT)'
        Popen.return_value.communicate.return_value = (sql, None)
        source.execute.return_value.fetchone.return_value = None
        ret = self.get_migrator()._get_ddls()
        self.assertEqual(ret, sql)

    @patch('pyvertica.migrate.subprocess.Popen', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source_con', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    def test__get_ddls_empty_with_pwd(self, source, source_con, Popen):
        """
        Test :py:meth:`.VerticaMigrator._get_ddls`.
        None from DB, get from subprocess, password sent to vsql
        """
        sql = 'CREATE TABLE cheese (id INT)'
        Popen.return_value.communicate.return_value = (sql, None)
        source.execute.return_value.fetchone.return_value = None
        ret = self.get_migrator(source_pwd='tartiflette')._get_ddls()
        self.assertEqual(ret, sql)

    @patch('pyvertica.migrate.subprocess.Popen', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source_con', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    def test__get_ddls_empty_with_exception(self, source, source_con, Popen):
        """
        Test :py:meth:`.VerticaMigrator._get_ddls`.
        None from DB, exception in subprocess.
        """
        source.execute.return_value.fetchone.return_value = None
        Popen.side_effect = CalledProcessError(42, 'Boom')
        self.assertRaises(VerticaMigratorError, self.get_migrator()._get_ddls)

    @patch('pyvertica.migrate.subprocess.Popen', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source_con', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    def test__get_ddls_not_empty(self, source, source_con, Popen):
        """
        Test :py:meth:`.VerticaMigrator._get_ddls`.
        Result from DB.
        """
        Popen.return_value.communicate.return_value = ('something', None)
        ret = self.get_migrator()._get_ddls()
        self.assertEqual(ret, 'something')

    # ### SEQUENCE manipulation
    def test_sequence_regexp_valid(self):
        """
        Test regular expression VerticaMigrator._find_seq
        Test a valid syntax.
        """
        re = self.get_migrator()._find_seq.search
        seq = 'CREATE SEQUENCE schema.seq_name'
        m_seqs = re(seq)
        schema = m_seqs.group('schema')
        seqname = m_seqs.group('seq')
        self.assertEqual(schema, 'schema')
        self.assertEqual(seqname, 'seq_name')

    def test_sequence_regexp_invalid(self):
        """
        Test regular expression VerticaMigrator._find_seq
        Test an invalid syntax.
        """
        re = self.get_migrator()._find_seq.search
        seq = 'CREATE TABLE plop (id int)'
        m_seqs = re(seq)
        self.assertEqual(m_seqs, None)

    def test__is_sequence_true(self):
        """
        Test :py:meth:`.VerticaMigrator._is_sequence`,
        with a valid sequence
        """
        seq = self.get_migrator()._is_sequence(
            "CREATE SEQUENCE schema.seq_name"
        )
        self.assertEqual(seq, True)

    def test__is_sequence_false(self):
        """
        Test :py:meth:`.VerticaMigrator._is_sequence`,
        with a non-sequence DDL
        """
        seq = self.get_migrator()._is_sequence(
            "CREATE TABLE s.t (id INT)"
        )
        self.assertEqual(seq, False)

    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    def test_update_sequence_start(self, source):
        """
        Test :py:meth:`.VerticaMigrator._update_sequence_start`,
        """
        source.execute.return_value.fetchone.return_value = [42]
        seq = 'CREATE SEQUENCE schema.seq_name'
        res = self.get_migrator()._update_sequence_start(seq)
        self.assertEqual(res, seq + ' START WITH 43')

    # ### test IDENTITY

    def test_identity_regexp_valid(self):
        """
        Test regular expression VerticaMigrator._find_identity
        Test a valid syntax.
        """
        re = self.get_migrator()._find_identity.search
        ident = '''CREATE TABLE schema.cheese (
            id IDENTITY,
            runny INT)'''
        m_ids = re(ident)
        schema = m_ids.group('schema')
        table = m_ids.group('table')
        col = m_ids.group('col')
        self.assertEqual(schema, 'schema')
        self.assertEqual(table, 'cheese')
        self.assertEqual(col, 'id')

    def test_identity_regexp_invalid(self):
        """
        Test regular expression VerticaMigrator._find_identity
        Test an invalid syntax.
        """
        re = self.get_migrator()._find_identity.search
        ident = '''CREATE TABLE schema.cheese (
            id INT,
            runny INT)'''
        m_ids = re(ident)
        self.assertEqual(m_ids, None)

    def test__uses_identity_true(self):
        """
        """
        seq_true = self.get_migrator()._uses_identity(
            '''CREATE TABLE schema.cheese (
                id IDENTITY,
                runny BOOLEAN
                )
                '''
        )
        self.assertEqual(seq_true, True)

    def test__uses_identity_false(self):
        seq_false = self.get_migrator()._uses_identity(
            '''CREATE TABLE schema.cheese (
                id INT,
                runny BOOLEAN
                )
                '''
        )
        self.assertEqual(seq_false, False)

    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    def test__replace_initialised_identity(self, source):

        source.execute.return_value.fetchone.side_effect = \
            [['cheezy_seq'], [42]]

        ddl, new_id = self.get_migrator()._replace_identity(
            '''CREATE TABLE schema.cheese (
                id IDENTITY,
                runny BOOLEAN
                )
                '''
        )
        self.assertEqual(ddl, '''CREATE TABLE schema.cheese (
                id INT NOT NULL,
                runny BOOLEAN
                )
                ''')
        self.assertEqual(new_id, {'schema': 'schema', 'table': 'cheese',
                                  'col': 'id', 'start':  43,
                                  'name': 'cheezy_seq'})

    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    def test__replace_uninitialised_identity(self, source):
        source.execute.return_value.fetchone.side_effect = \
            [['cheezy_seq'], [None]]
        ddl, new_id = self.get_migrator()._replace_identity(
            '''CREATE TABLE schema.cheese (
                id IDENTITY,
                runny BOOLEAN
                )
                '''
        )
        self.assertEqual(ddl, '''CREATE TABLE schema.cheese (
                id INT NOT NULL,
                runny BOOLEAN
                )
                ''')
        self.assertEqual(new_id, {'schema': 'schema', 'table': 'cheese',
                                  'col': 'id', 'start':  1,
                                  'name': 'cheezy_seq'})

    # ### test TEMPORARY TABLE
    def test__is_tmptable_true(self):
        tmp_true = self.get_migrator()._is_temporary_table('''
            CREATE TEMPORARY TABLE schema.something (a INT)
            ''')
        self.assertEqual(tmp_true, True)

    def test__is_tmptable_false(self):
        tmp_false = self.get_migrator()._is_temporary_table('''
            CREATE TABLE schema.something
            blah
            ''')
        self.assertEqual(tmp_false, False)

    # ### test PROJECTION

    def test__is_proj_true(self):
        proj_true = self.get_migrator()._is_proj('''
            CREATE PROJECTION schema.something
            ( col1, col2) AS SELECT * FROM schema.table
            SEGMENTED BY hash(cols) ALL NODES OFFSET 0;
            ''')
        self.assertEqual(proj_true, True)

    def test__is_proj_false(self):
        proj_false = self.get_migrator()._is_proj('''
            CREATE TABLE schema.something
            blah
            ''')
        self.assertEqual(proj_false, False)

    # ### Get table list

    # get_table_list can not be unit tested in a useful way.

    ### Connection type
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    def test__connection_type(self, target, source):

        migrator = self.get_migrator()
        self.assertEqual('direct', migrator._connection_type())

        source.execute.side_effect = Exception('Cannot Connect')
        self.assertEqual('odbc', migrator._connection_type())

    ### DDL Migration

    def test__exec_ddl_already_exists(self):
        migrator = self.get_migrator(clever_ddls=True)
        migrator._commit = True
        migrator._target = Mock()
        migrator._target.execute.side_effect = pyodbc.ProgrammingError(
            '42601', 'message')
        migrator._exec_ddl('DDL')

    def test__exec_ddl_other_error(self):
        migrator = self.get_migrator(clever_ddls=True)
        migrator._commit = True
        migrator._target = Mock()
        migrator._target.execute.side_effect = pyodbc.ProgrammingError(
            '42', 'message')

        self.assertRaises(pyodbc.ProgrammingError, migrator._exec_ddl, 'DDL')

    def test_migrate_ddls_None(self):
        migrator = self.get_migrator()
        migrator._get_ddls = Mock(return_value=None)
        migrator._exec_ddl = Mock()
        migrator.migrate_ddls()
        self.assertEqual(0, migrator._exec_ddl.call_count)

    def test_migrate_ddls_empty(self):
        migrator = self.get_migrator()
        migrator._get_ddls = Mock(return_value='')
        migrator._exec_ddl = Mock()
        migrator.migrate_ddls()
        self.assertEqual(0, migrator._exec_ddl.call_count)

    def test_migrate_ddls_proj(self):
        migrator = self.get_migrator()
        migrator._get_ddls = Mock(return_value='CREATE PROJECTION')
        migrator._exec_ddl = Mock()
        migrator.migrate_ddls()
        self.assertEqual(0, migrator._exec_ddl.call_count)

    def test_migrate_ddls_tmptable(self):
        migrator = self.get_migrator()
        migrator._get_ddls = Mock(return_value='CREATE TEMPORARY TABLE')
        migrator._exec_ddl = Mock()
        migrator.migrate_ddls()
        self.assertEqual(0, migrator._exec_ddl.call_count)

    def test_migrate_ddls_sequence(self):
        migrator = self.get_migrator()
        migrator._get_ddls = Mock(return_value='not empty')
        migrator._is_proj = Mock(return_value=False)
        migrator._is_sequence = Mock(return_value=True)
        migrator._update_sequence_start = Mock(return_value='CREATE SEQ')
        migrator._uses_identity = Mock(return_value=False)
        migrator._exec_ddl = Mock()
        migrator.migrate_ddls()
        migrator._exec_ddl.assert_called_once_with(
            migrator._update_sequence_start.return_value)

    def test_migrate_ddls_identity(self):
        migrator = self.get_migrator()
        migrator._commit = True
        migrator._get_ddls = Mock(return_value='not empty')
        migrator._is_proj = Mock(return_value=False)
        migrator._is_sequence = Mock(return_value=False)
        migrator._uses_identity = Mock(return_value=True)
        migrator._replace_identity = Mock(return_value=['WITH IDENTITY',
                                                        {'schema': 's',
                                                         'name': 'n',
                                                         'start': 's',
                                                         'table': 't',
                                                         'col': 'c'}])

        migrator._exec_ddl = Mock()
        migrator.migrate_ddls()
        self.assertEqual(call('WITH IDENTITY'),
                         migrator._exec_ddl.call_args_list[0])
        self.assertEqual(3, migrator._exec_ddl.call_count)

    def test_migrate_ddls_error(self):
        migrator = self.get_migrator()
        migrator._commit = True
        migrator._get_ddls = Mock(return_value='CREATE VIEW')
        migrator._exec_ddl = Mock()
        migrator._exec_ddl.side_effect = Exception('Boom')

        self.assertRaises(VerticaMigratorError, migrator.migrate_ddls)
        # 1st call => error, go to errors
        # 2nd call => error again, errors do no shrink
        # 3rd call => to display
        self.assertEqual(3, migrator._exec_ddl.call_count)

    # ### Data migration
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    def test__migrate_table_direct(self, target, source):
        migrator = self.get_migrator()
        migrator._commit = True
        migrator._migrate_table('direct', 'a.table', {'db': 'db'})
        assert(migrator._source.execute.call_args_list[0][0][0].startswith(
            'EXPORT'))

    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaBatch', Mock())
    def test__migrate_table_odbc(self, target, source):
        migrator = self.get_migrator()
        migrator._commit = True
        source.fetchone.side_effect = ['1', None]
        migrator._migrate_table('odbc', 'a.table', {'db': 'db'})
        assert(migrator._source.execute.call_args_list[0][0][0].startswith(
            'AT EPOCH'))

    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaBatch', Mock())
    def test__migrate_table_other(self, target, source):
        migrator = self.get_migrator()
        self.assertRaises(VerticaMigratorError, migrator._migrate_table,
                          'something', 'a.table', {'db': 'db'})

    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    def test_migrate_data_ok(self, target, source):
        migrator = self.get_migrator()
        migrator._connection_type = Mock()
        migrator._connection_details = Mock()
        migrator._get_table_list = Mock(return_value=['s.t'])
        migrator._migrate_table = Mock()
        self.assertEqual(migrator.migrate_data(''), None)

    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.time.sleep', Mock())
    def test_migrate_data_error(self, target, source):
        migrator = self.get_migrator()
        migrator._connection_type = Mock()
        migrator._connection_details = Mock()
        migrator._get_table_list = Mock(return_value=['s.t'])
        migrator._migrate_table = Mock()
        migrator._migrate_table.side_effect = pyodbc.ProgrammingError(42, 'm')
        migrator.migrate_data('')
        self.assertEqual(migrator.migrate_data(''), None)

    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.time.sleep', Mock())
    def test_migrate_data_bigerror(self, target, source):
        migrator = self.get_migrator()
        migrator._connection_type = Mock()
        migrator._connection_details = Mock()
        migrator._get_table_list = Mock(return_value=['s.t', 'not.done'])
        migrator._migrate_table = Mock()
        migrator._migrate_table.side_effect = Exception(42, 'm')
        self.assertRaises(Exception, migrator.migrate_data, '')
