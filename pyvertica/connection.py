import pyodbc


def get_connection(dsn, reconnect=True, **kwargs):
    """
    Get :py:mod:`!pyodbc` connection for the given ``dsn``.

    Usage example::

        from pyvertica.connection import get_connection


        connection = get_connection('TestDSN')
        cursor = connection.cursor()

    The connection will be made in two steps (with the assumption that you are
    connection via a load-balancer). The first step is connecting to the
    load-balancer and selecting a random node address. Then it will connect
    to that specific node and return this connection instance. This is done
    to avoid that all the data has to pass the load-balancer.

    .. note:: At this point it is expected that you have a ``odbc.ini`` file
        on your machine, defining the given ``dsn``.

    :param dsn:
        A ``str`` representing the data source name.

    :param reconnect:
        A ``boolean`` asking to reconnect to skip load balancer.


    :param kwargs:
        Keyword arguments accepted by the :py:mod:`!pyodbc` module.
        See: http://code.google.com/p/pyodbc/wiki/Module#connect

    :return:
        Return an instance of :class:`!pyodbc.Connection`.

    """
    print 'connection to dsn = ' + dsn + ' with reco = ' + str(reconnect)
    connection = pyodbc.connect('DSN={0}'.format(dsn), **kwargs)

    if reconnect:
        return get_connection(
            dsn, reconnect=False, servername=_get_random_node_address(connection), **kwargs)

    return connection


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


def connection_details(con):
    """
    Given one connection objects returns information about it.

    :param con:
        A :py:func:`!pyodbc.connect` object # THIS IS WRONG

    return:
        A ``dict`` of information, containing current user, host and database.
    """
    details = con.execute('''
        SELECT
            n.node_address as host
          , CURRENT_USER() as user
          , CURRENT_DATABASE() as db
        FROM v_monitor.current_session cs
        JOIN v_catalog.nodes n ON n.node_name=cs.node_name
        ''').fetchone()
    return {
        'host': details.host,
        'user': details.user,
        'db': details.db
    }
