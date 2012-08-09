import unittest2 as unittest

from mock import patch

from pyvertica.connection import get_connection


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
