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
        Return of the :py:func:`!pyodbc.connect` function.

    """
    return pyodbc.connect('DSN={0}'.format(dsn), **kwargs)


def connection_details(con):
    """
    Given one connection objects returns information about it.

    Usage example:

        from pyverti,ca.conenction import get_connection, connection_details

        con = get_connection('VerticaSTG')
        details = connection_details(con)

        :param con:
            A :py:func:`!pyodbc.connect` object # THIS IS WRONG

        :return:
            A ``dict`` of information, containing current user and host.
    """
    host = con.execute('''
        SELECT n.node_address
        FROM v_monitor.current_session cs
        JOIN v_catalog.nodes n ON n.node_name=cs.node_name
        ''').fetchone()[0]
    return {
        'host': host,
        'user': con.execute('SELECT CURRENT_USER()').fetchone()[0],
        'db': con.execute('SELECT CURRENT_DATABASE()').fetchone()[0],
    }
