

import argparse
import copy
import unittest2 as unittest

from subprocess import CalledProcessError
from mock import Mock, call, patch
from pyvertica.migrate import VerticaMigrator, VerticaMigratorException


class VerticaMigratorExceptionTest(unittest.TestCase):
    def test_exception(self):
        e = VerticaMigratorException('apenzeller')
        self.assertEqual(e.value, 'apenzeller')
        self.assertEqual(e.__str__(), "'apenzeller'")


class VerticaMigratorConnection(unittest.TestCase):
    """
    Test for the connection steps of :py:class:`.VerticaMigrator`.
    Another class will test the rest, mocking out globally the connections
    """


class VerticaMigratorTestCase(unittest.TestCase):
    """
    Test for :py:class:`.VerticaMigrator`.
    """

    def get_migrator(self, **kwargs):
        arguments = copy.deepcopy(argparse.Namespace())

        for k, v in kwargs.items():
            setattr(arguments, k, v)
        return VerticaMigrator('SourceDSN', 'TargetDSN', False, arguments)

    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # def test___init(self, cnx, sanity):
    #     """
    #     Test initialization of :py:class:`.VerticaMigrator`.
    #     """
    #     migrator = self.get_migrator()

    #     # variables
    #     self.assertEqual('SourceDSN', migrator._source_dsn)
    #     self.assertEqual('TargetDSN', migrator._target_dsn)
    #     self.assertEqual(False, migrator._commit)
    #     self.assertEqual(argparse.Namespace(), migrator._args)

    # ### Test connections to source and target

    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # @patch('pyvertica.migrate.get_connection')
    # def test__set_connections(self, cnx, sanity):
    #     """
    #     Test :py:meth:`.VerticaMigrator._set_connections`.
    #     """
    #     self.get_migrator()

    #     self.assertEqual([
    #         call('SourceDSN'),
    #         call('TargetDSN'),
    #     ], cnx.call_args_list)

    # ### different sanity options.
    # @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target_ip', '5.6.7.8', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # def test__sanity_ok(self, cnx, target):
    #     """
    #     Test :py:meth:`.VerticaMigrator._sanity_checks`.
    #     No exception should be raised if IP are different.
    #     """
    #     target.execute.return_value.fetchone.return_value = [0]
    #     self.get_migrator()

    # @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target_ip', '1.2.3.4', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # def test__sanity_same_ip_diff_db(self, cnx, target, source):
    #     """
    #     Test :py:meth:`.VerticaMigrator._sanity_checks`.
    #     No exception should be raised if IPs are identical but DBs are different.
    #     """
    #     target.execute.return_value.fetchone.side_effect = [['targetDB'], [0]]
    #     source.execute.return_value.fetchone.return_value = ['sourceDB']
    #     self.get_migrator()

    # @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target_ip', '1.2.3.4', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # def test__sanity_same_ip_same_db(self, cnx, target, source):
    #     """
    #     Test :py:meth:`.VerticaMigrator._sanity_checks`.
    #     Exception if IPs are identical and DBs are identical.
    #     """

    #     target.execute.return_value.fetchone.side_effect = [['targetDB'], [0]]
    #     source.execute.return_value.fetchone.return_value = ['targetDB']
    #     self.assertRaises(VerticaMigratorException, self.get_migrator)

    # @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target_ip', '5.6.7.8', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # def test__sanity_not_empty(self, cnx, target):
    #     """
    #     Test :py:meth:`.VerticaMigrator._sanity_checks`.
    #     Exception if target DB is not empty.
    #     """

    #     target.execute.return_value.fetchone.return_value = [42]
    #     self.assertRaises(VerticaMigratorException, self.get_migrator)

    # @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target_ip', '5.6.7.8', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # def test__sanity_not_empty_but_thats_ok(self, cnx, target):
    #     """
    #     Test :py:meth:`.VerticaMigrator._sanity_checks`.
    #     No exception if target DB is not empty and we know it: even_not_empty=True.
    #     """
    #     target.execute.return_value.fetchone.return_value = [42]
    #     self.get_migrator(even_not_empty=True)

    # ### get DDLs

    # @patch('subprocess.check_output')
    # @patch('pyvertica.migrate.VerticaMigrator._source_con', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__get_ddls_empty(self, checks, cnx, source, source_con, subp):
    #     """
    #     Test :py:meth:`.VerticaMigrator._get_ddls`.
    #     None from DB, get from subprocess.
    #     """
    #     sql = 'CREATE TABLE cheese (id INT)'
    #     subp.return_value = sql
    #     source.execute.return_value.fetchone.return_value = None
    #     ret = self.get_migrator()._get_ddls()
    #     self.assertEqual(ret, sql)

    # @patch('subprocess.check_output')
    # @patch('pyvertica.migrate.VerticaMigrator._source_con', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__get_ddls_empty_with_pwd(self, checks, cnx, source, source_con, subp):
    #     """
    #     Test :py:meth:`.VerticaMigrator._get_ddls`.
    #     None from DB, get from subprocess, password sent to vsql
    #     """
    #     sql = 'CREATE TABLE cheese (id INT)'
    #     subp.return_value = sql
    #     source.execute.return_value.fetchone.return_value = None
    #     ret = self.get_migrator(source_pwd='tartiflette')._get_ddls()
    #     self.assertEqual(ret, sql)

    # @patch('subprocess.check_output')
    # @patch('pyvertica.migrate.VerticaMigrator._source_con', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__get_ddls_empty_with_exception(self, checks, cnx, source, source_con, subp):
    #     """
    #     Test :py:meth:`.VerticaMigrator._get_ddls`.
    #     None from DB, exception in subprocess.
    #     """
    #     source.execute.return_value.fetchone.return_value = None
    #     subp.side_effect = CalledProcessError(42, 'Boom')
    #     self.assertRaises(VerticaMigratorException, self.get_migrator()._get_ddls)

    # @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__get_ddls_not_empty(self, checks, cnx, source):
    #     """
    #     Test :py:meth:`.VerticaMigrator._get_ddls`.
    #     Result from DB.
    #     """
    #     sql = 'CREATE TABLE cheese (id INT'
    #     source.execute.return_value.fetchone.return_value = [sql]
    #     ret = self.get_migrator()._get_ddls()
    #     self.assertEqual(ret, sql)

    # ### SEQUENCE manipulation

    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test_sequence_regexp_valid(self, checks, cnx):
    #     """
    #     Test regular expression VerticaMigrator._find_seqs
    #     Test a valid syntax.
    #     """
    #     re = self.get_migrator()._find_seqs.search
    #     seq = 'CREATE SEQUENCE schema.seq_name'
    #     m_seqs = re(seq)
    #     schema = m_seqs.group('schema')
    #     seqname = m_seqs.group('seq')
    #     self.assertEqual(schema, 'schema')
    #     self.assertEqual(seqname, 'seq_name')

    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test_sequence_regexp_invalid(self, checks, cnx):
    #     """
    #     Test regular expression VerticaMigrator._find_seqs
    #     Test an invalid syntax.
    #     """
    #     re = self.get_migrator()._find_seqs.search
    #     seq = 'CREATE TABLE plop (id int)'
    #     m_seqs = re(seq)
    #     self.assertEqual(m_seqs, None)

    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__is_sequence_true(self, checks, cnx):
    #     """
    #     Test :py:meth:`.VerticaMigrator._is_sequence`,
    #     with a valid sequence
    #     """
    #     seq = self.get_migrator()._is_sequence(
    #         "CREATE SEQUENCE schema.seq_name"
    #     )
    #     self.assertEqual(seq, True)

    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__is_sequence_false(self, checks, cnx):
    #     """
    #     Test :py:meth:`.VerticaMigrator._is_sequence`,
    #     with a non-sequence DDL
    #     """
    #     seq = self.get_migrator()._is_sequence(
    #         "CREATE TABLE s.t (id INT)"
    #     )
    #     self.assertEqual(seq, False)

    # @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test_update_sequence_start(self, checks, cnx, source):
    #     """
    #     Test :py:meth:`.VerticaMigrator._update_sequence_start`,
    #     """
    #     source.execute.return_value.fetchone.return_value = [42]
    #     seq = 'CREATE SEQUENCE schema.seq_name'
    #     res = self.get_migrator()._update_sequence_start(seq)
    #     self.assertEqual(res, seq + ' START WITH 43')

    # ### test IDENTITY

    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test_identity_regexp_valid(self, checks, cnx):
    #     """
    #     Test regular expression VerticaMigrator._find_identity
    #     Test an invalid syntax.
    #     """
    #     re = self.get_migrator()._find_identity.search
    #     ident = '''CREATE TABLE schema.cheese (
    #         id IDENTITY,
    #         runny INT)'''
    #     m_ids = re(ident)
    #     schema = m_ids.group('schema')
    #     table = m_ids.group('table')
    #     col = m_ids.group('col')
    #     self.assertEqual(schema, 'schema')
    #     self.assertEqual(table, 'cheese')
    #     self.assertEqual(col, 'id')

    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test_identity_regexp_invalid(self, checks, cnx):
    #     """
    #     Test regular expression VerticaMigrator._find_identity
    #     Test an invalid syntax.
    #     """
    #     re = self.get_migrator()._find_identity.search
    #     ident = '''CREATE TABLE schema.cheese (
    #         id INT,
    #         runny INT)'''
    #     m_ids = re(ident)
    #     self.assertEqual(m_ids, None)

    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__uses_identity(self, checks, cnx):
    #     """
    #     """
    #     seq_true = self.get_migrator()._uses_identity(
    #         '''CREATE TABLE schema.cheese (
    #             id IDENTITY,
    #             runny BOOLEAN
    #             )
    #             '''
    #     )
    #     self.assertEqual(seq_true, True)

    #     seq_false = self.get_migrator()._uses_identity(
    #         '''CREATE TABLE schema.cheese (
    #             id INT,
    #             runny BOOLEAN
    #             )
    #             '''
    #     )
    #     self.assertEqual(seq_false, False)

    # @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__replace_identity(self, checks, cnx, source):

    #     source.execute.return_value.fetchone.side_effect = [['cheezy_seq'], [42], ['cheezy_seq'], [None]]

    #     ddl, new_id = self.get_migrator()._replace_identity(
    #         '''CREATE TABLE schema.cheese (
    #             id IDENTITY,
    #             runny BOOLEAN
    #             )
    #             '''
    #     )
    #     self.assertEqual(ddl, '''CREATE TABLE schema.cheese (
    #             id INT NOT NULL,
    #             runny BOOLEAN
    #             )
    #             ''')
    #     self.assertEqual(new_id, {'schema': 'schema',
    #                 'table': 'cheese',
    #                 'col': 'id',
    #                 'start':  43,
    #                 'name': 'cheezy_seq'
    #                 })

    #     ddl, new_id = self.get_migrator()._replace_identity(
    #         '''CREATE TABLE schema.cheese (
    #             id IDENTITY,
    #             runny BOOLEAN
    #             )
    #             '''
    #     )
    #     self.assertEqual(ddl, '''CREATE TABLE schema.cheese (
    #             id INT NOT NULL,
    #             runny BOOLEAN
    #             )
    #             ''')
    #     self.assertEqual(new_id, {'schema': 'schema',
    #                 'table': 'cheese',
    #                 'col': 'id',
    #                 'start':  1,
    #                 'name': 'cheezy_seq'
    #                 })

    # ### test PROJECTION

    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__is_proj(self, checks, cnx):
    #     proj_true = self.get_migrator()._is_proj('''
    #         CREATE PROJECTION schema.something
    #         ( col1, col2) AS SELECT * FROM schema.table
    #         SEGMENTED BY hash(cols) ALL NODES OFFSET 0;
    #         ''')
    #     self.assertEqual(proj_true, True)
    #     proj_false = self.get_migrator()._is_proj('''
    #         CREATE TABLE schema.something
    #         blah
    #         ''')
    #     self.assertEqual(proj_false, False)

    # ### Get table list

    # @patch('pyvertica.migrate.get_connection')
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__get_table_list(self, checks, cnx, con):
    #     base_sql = 'SELECT table_schema as s, table_name as t FROM tables WHERE is_system_table=false AND is_temp_table=false'

    #     self.get_migrator()._get_table_list(con, [])
    #     self.get_migrator()._get_table_list(con, ['dv', 'stg.table'])
    #     self.assertEqual([
    #         call(base_sql),
    #         call(base_sql + " AND ((table_schema='dv') OR (table_schema='stg' AND table_name='table'))"),
    #     ], con.execute.call_args_list)

    # ### Connection type
    # @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    # @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    # @patch('pyvertica.migrate.connection_details')
    # @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    # @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    # def test__connection_type(self, checks, cnx, details, target, source):
    #     import pyodbc

    #     migrator = self.get_migrator()
    #     migrator._args.target_host = None
    #     migrator._args.target_pwd = 'pwd'
    #     migrator._args.target_port = '5433'
    #     ctype_direct = migrator._connection_type()
    #     self.assertEqual(ctype_direct, 'direct')

    #     migrator._args.target_host = 'host'
    #     source.execute.side_effect = pyodbc.Error('Cannot Connect')
    #     ctype_odbc = migrator._connection_type()
    #     self.assertEqual(ctype_odbc, 'odbc')

    ### DDL migration
    @patch('pyvertica.migrate.VerticaMigrator._replace_identity')
    @patch('pyvertica.migrate.VerticaMigrator._uses_identity', autospec=True)
    @patch('pyvertica.migrate.VerticaMigrator._update_sequence_start')
    @patch('pyvertica.migrate.VerticaMigrator._is_sequence', autospec=True)
    @patch('pyvertica.migrate.VerticaMigrator._find_proj', autospec=True)
    @patch('pyvertica.migrate.VerticaMigrator._is_proj', autospec=True)
    @patch('pyvertica.migrate.VerticaMigrator._get_ddls')
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    def test_migrate_ddls(self, checks, cnx, target, get_ddls,
    is_proj, find_proj,
     is_seq, upd_seq,
      uses_ident,
      rep_ident):

        # # No ddl, empty
        # get_ddls.return_value = ''
        # self.get_migrator().migrate_ddls([])
        # self.assertEqual(target.execute.called, 0)

        # # No ddl, None
        # get_ddls.return_value = None
        # self.get_migrator().migrate_ddls([])
        # self.assertEqual(target.execute.called, 0)

        # # Should never happen but accounted for anyway
        # get_ddls.return_value = ';'
        # self.get_migrator().migrate_ddls([])
        # self.assertEqual(target.execute.called, 0)

        # proj only
        # get_ddls.return_value = 'CREATE PROJECTION schema.proj'
        # self.get_migrator().migrate_ddls([])
        # self.assertEqual(target.execute.called, False)
        # self.assertEqual(is_proj.called, True)

        # sequence
        get_ddls.return_value = '''CREATE SEQUENCE schema.seq_name
        '''
        is_proj.return_value = False
        self.get_migrator().migrate_ddls([])
        self.assertEqual(target.execute.called, False)
        self.assertEqual(is_seq.called, True)
        self.assertEqual(upd_seq.called, True)


