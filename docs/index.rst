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

This package contains a command-line utility called *vertica_batch_import*.
Usage::

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


This package contains a command-line utility called *vertica_migrate*.
Usage::

    usage: vertica_migrate [-h] [--commit]
                           [--log {debug,info,warning,error,critical}]
                           [--target_pwd TARGET_PWD] [--skip_ddls] [--clever_ddls]
                           [--skip_data] [--even_not_empty] [--limit LIMIT]
                           [--truncate] [--source_pwd SOURCE_PWD]
                           [--source_not_reconnect] [--target_not_reconnect]
                           [--target_host TARGET_HOST]
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
      --log {debug,info,warning,error,critical}
                            loglevel of loghandler (default: info)
      --target_pwd TARGET_PWD
                            Password of the target vertica. Used only to export
                            data via the vertica CONNECT statement
      --skip_ddls           Do not copy the DDLs over.
      --clever_ddls         If when copying a DDL an object with the same name
                            already exists, skip the copy.
      --skip_data           Do not copy the data over.
      --even_not_empty      Do not stop if the target DB is not empty.
      --limit LIMIT         Limit the number of rows to copy over, per table.
      --truncate            Truncate destination tables before copying data over.
      --source_pwd SOURCE_PWD
                            Password of the source vertica. Needed only if vsql
                            needs to be used to get the ddls.
      --source_not_reconnect
                            Do not try to avoid load balancer by reconnecting.
      --target_not_reconnect
                            Do not try to avoid load balancer by reconnecting.
      --target_host TARGET_HOST
                            If the target name of the CONNECT statement needs to
                            be given.


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
