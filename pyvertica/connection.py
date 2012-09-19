import pyodbc


def get_connection(dsn, **kwargs):
    """
    Get :py:mod:`!pyodbc` connection for the given ``dsn``.

    Usage example::

        from pyvertica.connection import get_connection


        connection = get_connection('VerticaSTG')
        cursor = connection.cursor()

    .. note:: At this point it is expected that you have a ``odbc.ini`` file
        on your machine, defining the given ``dsn``.

    :param dsn:
        A ``str`` representing the data source name.

    :param kwargs:
        Keyword arguments accepted by the :py:mod:`!pyodbc` module.
        See: http://code.google.com/p/pyodbc/wiki/Module#connect

    :return:
        Return an instance of :class:`!pyodbc.Connection`.

    """
    return pyodbc.connect('DSN={0}'.format(dsn), **kwargs)


def _get_random_node_address(connection):
    """
    Return the address of a random node in the cluster.

    :param connection:
        An instance of :class:`!pyodbc.Connection`.

    :return:
        A ``str`` representing the address of the node.

    """
    cursor = connection.cursor()
    cursor.execute(
        'SELECT node_address FROM nodes WHERE node_state = ? '
        'ORDER BY RANDOM() LIMIT 1',
        'UP'
    )
    return cursor.fetchone().node_address
