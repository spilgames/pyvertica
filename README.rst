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

``pyvertica.migrate``
    Module managing export from one Vertica cluster to another.


Provided scripts
~~~~~~~~~~~~~~~~

``vertica_batch_import``
    Command-line interface for the ``pyvertica.batch`` module.

``vertica_migrate``
    Command-line interface for the ``pyvertica.migrate`` module.


Installation
------------

*pyvertica* can be installing by executing ``pip install pyvertica``.


Links
-----

* `documentation <https://pyvertica.readthedocs.org/en/latest/>`_
* `source <http://github.com/spilgames/pyvertica/>`_


Changes
-------

v1.5.0
~~~~~~

* **ADD**: ``rolllback`` method to ``VerticaBatch`` object.
* **ADD**: ``REJECTMAX`` option.
* **CHANGE**: ``VerticaBatch.get_errors`` now returns the number of errors
  instead of a ``bool`` indicating if there are errors.
* **CHANGE**: ``REJECTEDFILE`` option is now set to ``__debug__``.



v1.4.1
~~~~~~

* **CHANGE**: Make it possible to pass host, and credentials to the
  ``vertica_migrate`` tool by using an config file (to not expose credentials
  on the command-line).


v1.4.0
~~~~~~

* **CHANGE**: Make it more easy to pass more arguments to ``get_connection``
  through ``BaseImporter`` and ``VerticaBatch.`` Note that this is a backwards
  incompatible change as the arguments for ``BaseImporter``, ``VerticaBatch``
  and ``get_connection`` have changed.


v1.3.0
~~~~~~

* **FEATURE**: Add a migration module ``pyvertica.migrate``and script
  ``vertica_migrate``, to move data between clusters.
* **CHANGE**: Make the reconnect optional in ``VerticaBatch.get_connection``


v1.2.4
~~~~~~

* **CHANGE**: Make the execution of ``ANALYZE_CONSTRAINTS`` optional (executed
  when getting the errors).


v1.2.3
~~~~~~

* **FIX**: Handle exceptions raised inside the ``QueryThread`` so that the
  semaphore always gets released.

v1.2.2
~~~~~~

* **FIX**: Line-endings in file-object returned by ``get_errors``. All is now
  ``\n``.


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
