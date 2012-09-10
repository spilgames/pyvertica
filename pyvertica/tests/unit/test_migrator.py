

import argparse
import copy
import unittest2 as unittest

from subprocess import CalledProcessError
from mock import Mock, call, patch
from pyvertica.migrate import VerticaMigrator, VerticaMigratorException


class VerticaMigratorExceptionTest(unittest.TestCase):
    def test_exception(self):
        e=VerticaMigratorException('apenzeller')
        self.assertEqual(e.value, 'apenzeller')
        self.assertEqual(e.__str__(), "'apenzeller'")


class VerticaMigratorTestCase(unittest.TestCase):
    """
    Test for :py:class:`.VerticaMigrator`.
    """

    def get_migrator(self, **kwargs):
        arguments = copy.deepcopy(argparse.Namespace())

        for k, v in kwargs.items():
            setattr(arguments, k, v)
        return VerticaMigrator('SourceDSN', 'TargetDSN', False, arguments)

    @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test___init(self, cnx, sanity):
        """
        Test initialization of :py:class:`.VerticaBatch` without truncate.
        """
        migrator = self.get_migrator()

        # variables
        self.assertEqual('SourceDSN', migrator._source_dsn)
        self.assertEqual('TargetDSN', migrator._target_dsn)
        self.assertEqual(False, migrator._commit)
        self.assertEqual(argparse.Namespace(), migrator._args)


    @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    @patch('pyvertica.migrate.get_connection')
    def test__set_connections(self, cnx, sanity):
        migrator = self.get_migrator()

        self.assertEqual([
            call('SourceDSN'),
            call('TargetDSN'),
        ], cnx.call_args_list)

    @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip', '5.6.7.8', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_ok(self, cnx, target):
        target.execute.return_value.fetchone.return_value = [0]
        migrator = self.get_migrator()

    @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip', '1.2.3.4', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_same_ip_diff_db(self, cnx, target, source):
        target.execute.return_value.fetchone.side_effect = [['targetDB'], [0]]
        source.execute.return_value.fetchone.return_value = ['sourceDB']
        migrator = self.get_migrator()


    @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip', '1.2.3.4', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_same_ip_same_db(self, cnx, target, source):
        target.execute.return_value.fetchone.side_effect = [['targetDB'], [0]]
        source.execute.return_value.fetchone.return_value = ['targetDB']
        self.assertRaises(VerticaMigratorException, self.get_migrator)



    @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip', '5.6.7.8', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_not_empty(self, cnx, target):
        target.execute.return_value.fetchone.return_value = [42]
        self.assertRaises(VerticaMigratorException, self.get_migrator)


    @patch('pyvertica.migrate.VerticaMigrator._source_ip', '1.2.3.4', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target_ip', '5.6.7.8', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._target', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    def test__sanity_not_empty_but_thats_ok(self, cnx, target):
        target.execute.return_value.fetchone.return_value = [42]
        self.get_migrator(even_not_empty=True)

    @patch('subprocess.check_output')
    @patch('pyvertica.migrate.VerticaMigrator._source_con', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    def test__get_ddls_empty(self, checks, cnx, source, source_con, subp):
        source.execute.return_value.fetchone.return_value = None
        self.get_migrator()._get_ddls()

    @patch('subprocess.check_output')
    @patch('pyvertica.migrate.VerticaMigrator._source_con', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._source', create=True)
    @patch('pyvertica.migrate.VerticaMigrator._set_connections')
    @patch('pyvertica.migrate.VerticaMigrator._sanity_checks')
    def test__get_ddls_empty_with_exception(self, checks, cnx, source, source_con, subp):
        source.execute.return_value.fetchone.return_value = None
        subp.side_effect=CalledProcessError(42, 'Boom')
        #self.get_migrator()._get_ddls()
        self.assertRaises(VerticaMigratorException, self.get_migrator()._get_ddls)