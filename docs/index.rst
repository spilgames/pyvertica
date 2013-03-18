Welcome to PyVertica documentation!
===================================

*pyvertica* is a package which contains the shared logic for connecting
and writing to a Vertica database.


Installation
------------

*pyvertica* can be installing by executing ``pip install pyvertica``.

.. note:: When using the :py:class:`~pyvertica.importer.BaseImporter`, do
  not forget to create the batch history table. An SQL example can be found
  in the class documentation.


Links
-----

* `documentation <http://packages.python.org/pyvertica/>`_
* `source <http://github.com/spilgames/pyvertica/>`_


Command-line usage
------------------

``vertica_batch_import``
~~~~~~~~~~~~~~~~~~~~~~~~

Tool to import a CSV-like file directly into Vertica.

::

    usage: vertica_batch_import [-h] [--commit]
                                [--partial-commit-after PARTIAL_COMMIT_AFTER]
                                [--log {debug,info,warning,error,critical}]
                                [--truncate-table] [--delimiter DELIMITER]
                                [--enclosed-by ENCLOSED_BY] [--skip SKIP]
                                [--null NULL]
                                [--record-terminator RECORD_TEMINATOR]
                                dsn table_name file_path

    Vertica batch importer

    positional arguments:
      dsn                   ODBC data source name
      table_name            name of table (including schema, eg: staging.my_table)
      file_path             absolute path to the file to import

    optional arguments:
      -h, --help            show this help message and exit
      --commit              commit after import (without it will perform a dry-
                            run)
      --partial-commit-after PARTIAL_COMMIT_AFTER
                            partial commit after num of lines (default: 1000000)
      --log {debug,info,warning,error,critical}
                            loglevel of loghandler (default: info)
      --truncate-table      truncate table before import
      --delimiter DELIMITER
                            delimiter to split columns (default: ;)
      --enclosed-by ENCLOSED_BY
                            the quote character (default: ")
      --skip SKIP           number of lines to skip (default: 0)
      --null NULL           represents a null value (default: empty string)
      --record-terminator RECORD_TEMINATOR
                            specifies the end of a record (default: newline)


.. _vertica_migrate:

``vertica_migrate``
~~~~~~~~~~~~~~~~~~~

Tool to migrate data from one to another Vertica cluster.

::

    usage: vertica_migrate [-h] [--commit]
                           [--log-level {debug,info,warning,error,critical}]
                           [--skip-ddls] [--clever-ddls] [--skip-data]
                           [--even-not-empty] [--limit LIMIT] [--truncate]
                           [--source-not-reconnect] [--target-not-reconnect]
                           [--config-path CONFIG_PATH]
                           source target [objects [objects ...]]

    Vertica Migrator

    positional arguments:
      source                ODBC data source name
      target                ODBC data source name
      objects               List of objects (schemas or table) to migrate

    optional arguments:
      -h, --help            show this help message and exit
      --commit              commit DDLS and copy data (without it will perform a
                            dry-run)
      --log-level {debug,info,warning,error,critical}
                            loglevel of loghandler (default: info)
      --skip-ddls           Do not copy the DDLs over.
      --clever-ddls         If when copying a DDL an object with the same name
                            already exists, skip the copy.
      --skip-data           Do not copy the data over.
      --even-not-empty      Do not stop if the target DB is not empty.
      --limit LIMIT         Limit the number of rows to copy over, per table.
      --truncate            Truncate destination tables before copying data over.
      --source-not-reconnect
                            Do not try to avoid load balancer by reconnecting.
      --target-not-reconnect
                            Do not try to avoid load balancer by reconnecting.
      --config-path CONFIG_PATH
                            Absolute path to a config file (useful for storing
                            credentials).


To not expose passwords on the command-line, it is mandatory to pass them as
a config file (``--config-path``). Example::

  [vertica_migrate]
  target_pwd=targetpassword
  source_pwd=sourcepassword
  log_level=warning

Any command-line argument accepting a string (eg: ``--log-level warning``) is
accepted (eg: ``log_level=warning``). The following extra options are
available in the config-file:

target_user
    Username of the target Vertica database.

target_pwd
    Password of the target Vertica database.

target_host
    Hostname of the target Vertica database.

source_user
    Username of the source Vertica database.

source_pwd
    Password of the source Vertica database.

source_host
    Hostname of the target Vertica database.


Usage within Python code
------------------------

Creating a PYODBC connection to Vertica
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: pyvertica.connection
    :members:


Writing multiple lines in a batch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pyvertica.batch.VerticaBatch
    :members:


Base importer class
~~~~~~~~~~~~~~~~~~~

.. autoclass:: pyvertica.importer.BaseImporter
  :members:


Exceptions
~~~~~~~~~~

.. autoexception:: pyvertica.importer.BatchImportError

.. autoexception:: pyvertica.importer.AlreadyImportedError

.. autoexception:: pyvertica.migrate.VerticaMigratorError
