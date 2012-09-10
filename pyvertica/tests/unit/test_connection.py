import unittest2 as unittest

from mock import patch

from pyvertica.connection import get_connection, connection_details


class ModuleTestCase(unittest.TestCase):
    """
    Tests for :py:mod:`~pyvertica.connection`.
    """
    @patch('pyvertica.connection.pyodbc')
    def test_get_connection_not_cached(self, pyodbc):
        """
        Test :py:func:`.get_connection` without cache.
        """
        connection = get_connection('VerticaSTG', foo='bar', bar='foo')

        pyodbc.connect.assert_called_once_with(
            'DSN=VerticaSTG', foo='bar', bar='foo')
        self.assertEqual(pyodbc.connect.return_value, connection)

    @patch('pyvertica.connection.get_connection')
    def test_connection_details(self, con):
        con.execute.return_value.fetchone.side_effect = [['h'], ['u'], ['d']]
        details = connection_details(con)
        self.assertEqual(details, {
            'host':'h',
            'user':'u',
            'db':'d'
            })
