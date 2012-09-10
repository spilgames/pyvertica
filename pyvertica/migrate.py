import argparse
import logging
import re
import subprocess
from subprocess import CalledProcessError
import tempfile

from pyvertica.connection import get_connection, connection_details

logger = logging.getLogger(__name__)


class VerticaMigratorException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class VerticaMigrator(object):
    """
    Completely copy over (minus the projections) a vertica database to
    another, including DDLs and data.

    :param source:
        A ``str`` being the source DSN.

    :param target:
        A ``str`` being the targetDSN.

    :param commit:
        A ``boolean`` asking to commit or not the changes.

    :param args:
        A ``dict`` of extra parameters. It must contain:
        - target_pwd: password of the target vertica
        - target_port: port of the target vertica.
        Those 2 options are needed as I have not found a way to get them
        from pyodbc of the target vertica. The other connect option for data
        export (user, host) can be found via the connection to the target DSN.
    """

    # regexp to get name of the CREATE SEQUENCE statements
    _find_seqs = re.compile('^\s*CREATE SEQUENCE\s+(?P<schema>.*?)\.(?P<seq>.*?)\s*$')

    # regexp to find identity in the CREATE TABLE with IDENTITY statements
    # eg: CREATE TABLE schema.table ... colname IDENTITY...
    # Note: it is not possible to get more than one IDENTITY per table
    _find_identity = re.compile(
        '^\s*CREATE TABLE\s+(?P<schema>.*?)\.(?P<table>.*?)\s+.*^\s*(?P<col>.*?)\s+IDENTITY\s*,\s*$',
        re.MULTILINE + re.DOTALL)

    # check if we are creating a PROJECTION
    _find_proj = re.compile('^\s*CREATE PROJECTION.*')

    def __init__(self, source, target, commit=False, args=argparse.Namespace()):
        logger.debug(
            'Initializing VerticaMigrator from {0} to {1}'.format(source, target)
            )

        self._source_dsn = source
        self._target_dsn = target
        self._commit = commit
        self._args = args

        self._set_connections()

        self._sanity_checks()

    def _set_connections(self):
        """
        Setup db connections
        """

        self._source_con = get_connection(self._source_dsn)
        self._source = self._source_con.cursor()
        self._source_ip = self._source.execute(
            "SELECT n.node_address FROM v_monitor.current_session cs "
            "JOIN v_catalog.nodes n ON n.node_name=cs.node_name"
            ).fetchone()[0]

        self._target_con = get_connection(self._target_dsn)
        self._target = self._target_con.cursor()
        self._target_ip = self._target.execute(
            "SELECT n.node_address FROM v_monitor.current_session cs "
            "JOIN v_catalog.nodes n ON n.node_name=cs.node_name"
            ).fetchone()[0]

    def _sanity_checks(self):
        """
        """
        # copying from and to the same server is probably a bad idea, but let's
        # give the benefit of the doubt and check the DB
        if self._source_ip == self._target_ip:
            target_db = self._target.execute('SELECT CURRENT_DATABASE').fetchone()[0]
            source_db = self._source.execute('SELECT CURRENT_DATABASE').fetchone()[0]
            if target_db == source_db:
                raise VerticaMigratorException(
                    "Source and target database are the same. Will stop here."
                    )
            else:
                logger.info('Copying inside the same server to another DB.')

        # let's not copy over a not empty database
        is_target_empty = self._target.execute(
            "SELECT count(*) FROM tables WHERE is_system_table=false AND is_temp_table=false"
            ).fetchone()[0]

        if is_target_empty > 0:
            if 'even_not_empty' in self._args and self._args.even_not_empty:
                logger.info('Target DB not empty but copy anyway.')
            else:
                raise VerticaMigratorException("Target vertica is not empty.")

    def _get_ddls(self):
        """
        Query the source vertica to get the DDLs as a big string, using the
        EXPORT_OBJECTS function.

        It happens that this function returns None from odbc. In that case
        vsql is used, and the --source_pwd parameters becomes useful.

        :return:
            A ``str`` containg the DDLs.
        """
        logger.info('Getting DDLs...')
        from_db = self._source.execute("SELECT EXPORT_OBJECTS('', '', False)").fetchone()

        if from_db is None:
            details = connection_details(self._source_con)

            if 'source_pwd' in self._args and self._args.source_pwd is not None:
                details['pwd'] = self._args.source_pwd
            else:
                details['pwd'] = ''

            err = tempfile.TemporaryFile()
            try:
                ddls = subprocess.check_output([
                    '/opt/vertica/bin/vsql',
                    '-U', details['user'],
                    '-h', details['host'],
                    '-d', details['db'],
                    '-t',  # rows only
                    '-w',  details['pwd'],
                    '-c',  "SELECT EXPORT_OBJECTS('', '', False)"
                    ], stderr=err)
            except CalledProcessError as e:
                err.seek(0)
                raise VerticaMigratorException("""
                    Could not use vsql to get ddls: {0}.
                    Output was: {1}
                    """.format(e, err.read()))
        else:
            ddls = from_db.fetchone()[0]

        logger.info('Got DDLs' + ' (From vsql)' if from_db is None else '')
        return ddls

    def _uses_sequence(self, ddl):
        """
        Is the ddl in parameter a CREATE TABLE with an SEQUENCE?

        :param ddl:
            A ddl as a ``str``.

        :return:
            A ``boolean`` True is yes, False otherwise.
        """
        m_seqs = self._find_seqs.search(ddl)
        return m_seqs is not None

    def _update_sequence_start(self, ddl):
        """
        Called only if the ddl in parameters is a CREATE TABLE with SEQUENCE.
        It updates the SEQUENCE to add a START value.

        :param ddl:
            A ddl as a ``str``.

        :return:
            A new ddl ``str``.
        """
        m_seqs = self._find_seqs.search(ddl)
        schema = m_seqs.group('schema')
        seq = m_seqs.group('seq')

        current = self._source.execute(
            """
            SELECT current_value
            FROM v_catalog.sequences
            WHERE sequence_schema='{schema}' AND sequence_name='{seq}'
            """.format(schema=schema, seq=seq)
        ).fetchone()[0]

        if current == 0:
            current = 1
        ddl += ' START WITH {0}'.format(current)
        return ddl

    def _uses_identity(self, ddl):
        """
        Is the ddl in parameter a CREATE TABLE with an IDENTITY?

        :param ddl:
            A ddl as a ``str``.

        :return:
            A ``boolean`` True is yes, False otherwise.
        """
        m_ids = self._find_identity.search(ddl)
        return m_ids is not None

    def _replace_identity(self, ddl):
        """
        Called only if the ddl in parameters is a CREATE TABLE with iDENTITY.
        It replaces the iDENTITY by a INT NOT NULL and defines a sequence to
        be added later.

        :param ddl:
            A ddl as a ``str``.

        :return:
            A ``list`` of new ddl (``str``) and new sequence informattion
            (``dict``)
        """

        m_ids = self._find_identity.search(ddl)
        identity = None

        ddl = ddl.replace('IDENTITY', 'INT NOT NULL')

        col = m_ids.group('col')
        schema = m_ids.group('schema')
        table = m_ids.group('table')

        # get seq name
        seq_name = self._source.execute(
            """
            SELECT sequence_name
            FROM sequences
            WHERE sequence_schema='{schema}'
              AND identity_table_name='{table}'
            """.format(schema=schema, table=table)).fetchone()[0]

        # get current seq value
        max_seq = self._source.execute(
            'SELECT MAX({col}) FROM {schema}.{table}'.format(
                schema=schema, table=table, col=col)
            ).fetchone()[0]

        if max_seq is None:
            max_seq = 0

        identity = {'schema': schema,
                    'table': table,
                    'col': col,
                    'start': max_seq + 1,
                    'name': seq_name
                    }
        return ddl, identity

    def _is_proj(self, ddl):
            """
            """
            m_projs = self._find_proj.search(ddl)
            if m_projs:
                return True
            else:
                return False

    def migrate_ddls(self):
        """
        Migrates DDLs from the source to the target.
        Algo:
        - Get the full DDL from EXPORT_OBJECTS
          - Split by ; to get statement one by one
          - If the statement is a CREATE PROJECTION:
            - do nothing
          - If the statement is a CREATE SEQUENCE
             - find the current value which will thus be a START WITH
          - If the statement is a CREATE TABLE which define an IDENTITY
            - replace IDENTITY by INT NOT NULL
            - remember schema, table, column, sequence name, current sequence
            - create a sequence based on this
            - alter table to use the sequence
          - execute each statement (only if commit)
        """
        ddls = self._get_ddls()

        logger.info('Migrating DDLs...')
        for count, ddl in enumerate(ddls.split(';')):
            ddl = ddl.strip()

            # pyodbc or vertica statement hangs when executing ''
            if ddl == '' or ddl is None:
                continue

            # do not bother with copying projections over
            if self._is_proj(ddl):
                continue

            # do we need to find the start of a sequence?
            if self._uses_sequence(ddl):
                ddl = self._update_sequence_start(ddl)

            # Do we need to replace an IDENTITY by a sequence?
            new_seq = None
            if self._uses_identity(ddl):
                ddl, new_seq = self._replace_identity(ddl)

            # for display only: 1st line of statement, to display object name
            logger.warning(ddl.split('\n', 1)[0])

            if self._commit:
                self._target.execute(ddl)

            if new_seq is not None:
                create = 'CREATE SEQUENCE {schema}.{name} START WITH {start}'.format(
                    schema=new_seq['schema'],
                    name=new_seq['name'],
                    start=new_seq['start']
                    )
                logger.debug(create)
                if self._commit:
                    self._target.execute(create)

                alter = "ALTER TABLE {schema}.{table} ALTER COLUMN {col} SET DEFAULT NEXTVAL('{schema}.{name}')".format(
                    schema=new_seq['schema'],
                    name=new_seq['name'],
                    table=new_seq['table'],
                    col=new_seq['col'])
                logger.debug(alter)
                if self._commit:
                    self._target.execute(alter)
        logger.info('{0} DDLs migrated'.format(count))

    def migrate_data(self):
        """
        """
        logger.warning('Starting migrating data.')
        details = connection_details(self._target)

        if self._args.target_host is not None:
            details['host'] = self._args.target_host

        details['pwd'] = self._args.target_pwd
        details['port'] = self._args.target_port

        connect = "CONNECT TO VERTICA {db} USER {user} PASSWORD '{pwd}' ON '{host}',{port}".format(
                db=details['db'],
                user=details['user'],
                host=details['host'],
                pwd=details['pwd'],
                port=details['port'])
        logger.warning(connect)
        self._source.execute(connect)

        # as target and source should have the same shemas,
        # look at target to not screw up the source cursor
        self._target.execute("SELECT table_schema as s, table_name as t FROM tables where is_system_table=false and is_temp_table=false")

        while True:
            row = self._target.fetchone()
            if row is None:
                break
            sql = 'EXPORT TO VERTICA stgdwh.{s}.{t} FROM {s}.{t}'.format(s=row.s, t=row.t)
            logger.warning(sql + '...')
            nbrows = 0
            if self._commit:
                self._source.execute(sql)
                nbrows = self._source.rowcount
            logger.warning('%s rows exported.' % nbrows)

        logger.info('All data exported, disconnect')
        self._source.execute('DISCONNECT stgdwh')
