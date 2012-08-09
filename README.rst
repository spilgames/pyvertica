PyVertica
=========

This package contains tools for performing batch imports to the Vertica
database. See ``docs`` folder for full documentation.


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


Changes
-------

v1.1.2
~~~~~~

* Public release!
