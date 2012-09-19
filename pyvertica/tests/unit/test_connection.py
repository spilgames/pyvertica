import unittest2 as unittest

from mock import Mock, patch

from pyvertica.connection import get_connection, _get_random_node_address


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

    def test__get_random_node_address(self):
        """
        Test :py:func:`._get_random_node_address`.
        """
        connection = Mock()
        cursor = connection.cursor()
        row = cursor.fetchone.return_value

        self.assertEqual(
            row.node_address,
            _get_random_node_address(connection)
        )

        cursor.execute.assert_called_once_with(
            'SELECT node_address FROM nodes WHERE node_state = ? '
            'ORDER BY RANDOM() LIMIT 1',
            'UP'
        )
