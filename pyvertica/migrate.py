import logging
import re

from pyvertica.connection import get_connection

logger = logging.getLogger(__name__)


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
    # Note: it is not possible to get more than one iDENTITY per table
    _find_identity = re.compile(
        '^\s*CREATE TABLE\s+(?P<schema>.*?)\.(?P<table>.*?)\s+.*^\s*(?P<col>.*?)\s+IDENTITY\s*,\s*$',
        re.MULTILINE + re.DOTALL)

    # check if we are creating a PROJECTION
    _find_proj = re.compile('^\s*CREATE PROJECTION.*')

    def __init__(self, source, target, commit=False, args={}):
        logger.debug(
            'Initializing VerticaMigrator from {0} to {1}'.format(source, target)
            )

        self._source = source
        self._target = target
        self._commit = commit
        self._args = args

        # setup db connection
        self._source_db = get_connection(self._source)
        self._source = self._source_db.cursor()
        self._source_ip = self._source.execute(
            "SELECT n.node_address FROM v_monitor.current_session cs "
            "JOIN v_catalog.nodes n ON n.node_name=cs.node_name"
            ).fetchone()[0]

        self._target_db = get_connection(self._target)
        self._target = self._target_db.cursor()
        self._target_ip = self._target.execute(
            "SELECT n.node_address FROM v_monitor.current_session cs "
            "JOIN v_catalog.nodes n ON n.node_name=cs.node_name"
            ).fetchone()[0]

        # copying from and to the same server is probably a bad idea, but let's
        # give the benefit of the doubt
        if self._source_ip == self._target_ip:
            target_db = self._target.execute(
                'SELECT CURRENT_DATABASE'
                ).fetchone()[0]
            source_db = self._source.execute(
                'SELECT CURRENT_DATABASE'
                ).fetchone()[0]
            if target_db == source_db:
                logger.exception(
                    "Source and target database are the same. Will stop here."
                    )

        # let's not copy over a not empty database
        is_target_empty = self._target.execute(
            "SELECT count(*) FROM tables WHERE is_system_table=false AND is_temp_table=false"
            ).fetchone()[0]
        if is_target_empty > 0:
            logger.exception(
                "Target vertica is not empty."
            )

    def _get_ddls(self):
        """
        Query the source vertica to get the DDLs as a big string, using the
        EXPORT_OBJECTS function.

        :return:
            A ``str`` containg the DDLs.
        """
        ddls = self._source.execute("SELECT EXPORT_OBJECTS('', '', False)"
            ).fetchone()[0]

        if ddls is None or ddls == '':
            logger.exception("No ddls found. Todo: target vsql")

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

        for ddl in ddls.split(';'):
            ddl = ddl.strip()

            # pyodbc or vertica statement hangs when executing ''
            if ddl == '' or ddl is None:
                continue

            # do not bother with copyoing projections over
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
            logger.debug(ddl.split('\n', 1)[0])

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

    def migrate_data(self):
        """
        """

        user = self._target.execute('select CURRENT_USER()').fetchone()[0]
        # If we are playing with tunnels...
        if self._args.target_host is not None:
            host = self._args.target_host
        else:
            host = self._target.execute("select node_address FROM nodes WHERE node_state='UP' LIMIT 1").fetchone()[0]
        db = self._target.execute('select CURRENT_DATABASE()').fetchone()[0]
        pwd = self._args.target_pwd
        port = self._args.target_port

        self._source.execute(
            "CONNECT TO VERTICA {db} USER {user} PASSWORD '{pwd}' ON '{host}',{port}".format(
                db=db,
                user=user,
                host=host,
                pwd=pwd,
                port=port)
            )
        # as target and source should have the same shemas,
        # look at target to not screw up the source cursor
        self._target.execute("SELECT table_schema as s, table_name as t FROM tables where is_system_table=false and is_temp_table=false")

        while True:
            row = self._target.fetchone()
            if row is None:
                break
            sql = 'EXPORT TO VERTICA stgdwh.{s}.{t} FROM {s}.{t}'.format(s=row.s, t=row.t)
            print sql
            nbrows = 0
            if self._commit:
                self._source.execute(sql)
                nbrows = self._source.rowcount
            print '%s rows exported.' % nbrows

        print 'All done, disconnect'
        self._source.execute('DISCONNECT stgdwh')
