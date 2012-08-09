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
