PyVertica
=========

This package contains tools for performing batch imports to the Vertica
database.


Provided modules
~~~~~~~~~~~~~~~~

``pyvertica.connection``
    Module which contains logic for connection to a Vertica DB.

``pyvertica.batch``
    High speed loader for Vertica.

``pyvertica.importer``
    Base-class for writing Vertica batch importers.


Provided scripts
~~~~~~~~~~~~~~~~

``vertica_batch_import``
    Command-line interface for the ``pyvertica.vcopy`` module.


Installation
------------

*pyvertica* can be installing by executing ``pip install pyvertica``.


Links
-----

* `documentation <http://packages.python.org/pyvertica/>`_
* `source <http://github.com/spilgames/pyvertica/>`_


Changes
-------

v1.2.1
~~~~~~

* **CHANGE**: The Batch-history table is now configurable in ``BaseImporter``.
* **CHANGE**: ``get_connection`` selects a random node from the cluster
  and returns a connection to that node.
* **FIX**: Cleanup fifo + temporary directory.

v1.2.0
~~~~~~

* **CHANGE**: Change the way in how we detect if the FIFO object is consumed
  by the QueryThread. By opening the FIFO object (for writing) in ``'w'`` mode
  after the QueryThread was created, it will block until the COPY SQL statement
  is started.


v1.1.2
~~~~~~

* Public release!
