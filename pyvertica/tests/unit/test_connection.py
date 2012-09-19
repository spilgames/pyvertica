import unittest2 as unittest

from mock import Mock, call, patch

from pyvertica.connection import get_connection, _get_random_node_address


class ModuleTestCase(unittest.TestCase):
    """
    Tests for :py:mod:`~pyvertica.connection`.
    """
    @patch('pyvertica.connection._get_random_node_address')
    @patch('pyvertica.connection.pyodbc')
    def test_get_connection(self, pyodbc, get_random_node_address):
        """
        Test :py:func:`.get_connection`.
        """
        pyodbc.connect.side_effect = ['connection1', 'connection2']

        connection = get_connection('TestDSN', foo='bar', bar='foo')

        self.assertEqual([
            call('DSN=TestDSN', foo='bar', bar='foo'),
            call(
                'DSN=TestDSN',
                servername=get_random_node_address.return_value,
                foo='bar',
                bar='foo',
            ),
        ], pyodbc.connect.call_args_list)

        get_random_node_address.assert_called_once_with('connection1')

        self.assertEqual('connection2', connection)

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
