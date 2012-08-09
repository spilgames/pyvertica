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
