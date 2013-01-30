import logging
import re
import subprocess
import tempfile
from subprocess import CalledProcessError

import pyodbc

from pyvertica.connection import get_connection, connection_details
from pyvertica.batch import VerticaBatch

logger = logging.getLogger(__name__)


class VerticaMigratorError(Exception):
    """
    Raised randomly instead of killing kittens.
    """
    # def __init__(self, value):
    #     self.value = value

    # def __str__(self):
    #     return repr(self.value)


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
    _find_seqs = re.compile(
        '^\s*CREATE SEQUENCE\s+(?P<schema>.*?)\.(?P<seq>.*?)\s*$')

    # regexp to find identity in the CREATE TABLE with IDENTITY statements
    # eg: CREATE TABLE schema.table ... colname IDENTITY...
    # Note: it is not possible to get more than one IDENTITY per table
    _find_identity = re.compile(
        '^\s*CREATE TABLE\s+(?P<schema>.*?)\.(?P<table>.*?)'
        '\s+.*^\s*(?P<col>.*?)\s+IDENTITY\s*,\s*$',
        re.MULTILINE + re.DOTALL)

    # check if we are creating a PROJECTION
    _find_proj = re.compile('^\s*CREATE PROJECTION.*')

    def __init__(self, source, target, commit=False, **kwargs):
        logger.debug(
            'Initializing VerticaMigrator from {0} to {1}'.format(
                source, target))

        self._source_dsn = source
        self._target_dsn = target
        self._commit = commit
        self._args = kwargs
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
                raise VerticaMigratorError(
                    "Source and target database are the same. Will stop here."
                    )
            else:
                logger.info('Copying inside the same server to another DB.')

        # let's not copy over a not empty database
        is_target_empty = self._target.execute(
            "SELECT count(*) FROM tables WHERE is_system_table=false AND is_temp_table=false"
            ).fetchone()[0]

        if is_target_empty > 0:
            if 'even_not_empty' in self._args and self._args['even_not_empty']:
                logger.info('Target DB not empty but copy anyway.')
            else:
                raise VerticaMigratorError("Target vertica is not empty.")

    def _get_ddls(self, objects=[]):
        """
        Query the source vertica to get the DDLs as a big string, using the
        EXPORT_OBJECTS function.

        It happens that this function returns None from odbc. In that case
        vsql is used, and the --source_pwd parameters becomes useful.

        :return:
            A ``str`` containg the DDLs.
        """
        logger.info('Getting DDLs...')
        export_sql = "SELECT EXPORT_OBJECTS('', '{0}', False)".format(','.join(objects))

        # I often have a segfault when running this, so let's fallback by default
        # from_db = self._source.execute(export_sql).fetchone()
        from_db = None
        if from_db is None:
            logger.warning('Exporting object is done via vsql')
            details = connection_details(self._source_con)

            details['pwd'] = self._args.get('source_pwd', '')
            err = 'Unknown error'
            try:
                pr = subprocess.Popen([
                #ddls = subprocess.check_output([
                    '/opt/vertica/bin/vsql',
                    '-U', details['user'],
                    '-h', details['host'],
                    '-d', details['db'],
                    '-t',  # rows only
                    '-w',  details['pwd'],
                    '-c',  export_sql
                    ],  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                print pr.communicate()
                ddls, err = pr.communicate()
            except CalledProcessError as e:
                raise VerticaMigratorError("""
                    Could not use vsql to get ddls: {0}.
                    Output was: {1}
                    """.format(e, err))
        else:
            logger.info('From export_objects')
            ddls = from_db[0]

        return ddls

    def _is_sequence(self, ddl):
        """
        Is the ddl in parameter a CREATE SEQUENCE?

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

        current += 1
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
        Check if a DDL is a projection

        :param ddl:
            A ``str`` of DDLs.

        :return:
            A ``bool`` telling if the DDL is a projection or not.

        """
        m_projs = self._find_proj.search(ddl)
        if m_projs:
            return True
        else:
            return False

    def _get_table_list(self, con, objects):
        """
        Return a list of table constained in the object list in parameter.
        If object is empty it will mean all tables, if it is a schema it will
        return only the table of the schema, if it is a table it will only
        return the table itself.

        :param con:
            A pyodbc connection object.

        :return:
            A ``list`` of (schema, table) (``str``, ``str``).
        """
        # basic sql
        tables_sql = "SELECT table_schema as s, table_name as t FROM tables WHERE is_system_table=false AND is_temp_table=false"
        # extra where clause to find only specific tables
        where = []
        if len(objects) == 0:
            # Means all. We are happy with the default sql
            pass
        else:
            for o in objects:
                (schema, dot, table) = o.partition('.')
                if table == '':
                    # we have a schema only
                    where.append("table_schema='{s}'".format(s=schema))
                else:
                    # we have a table
                    where.append("table_schema='{s}' AND table_name='{t}'".format(t=table, s=schema))

        if len(where) > 0:
            tables_sql += ' AND ((' + ') OR ('.join(where) + '))'

        tret = con.execute(tables_sql).fetchall()
        return tret

    def _connection_type(self):
        """
        Finds out if the migration can be done directly via the EXPORT in
        Vertica, or if data needs to be loaded via odbc.

        :return:
            A ``str`` stating 'direct' or 'odbc'.
        """
        details = connection_details(self._target)

        details['pwd'] = self._args.get('target_pwd', '')

        connect = "CONNECT TO VERTICA {db} USER {user} PASSWORD '{pwd}' ON '{host}',5433".format(
                db=details['db'],
                user=details['user'],
                host=details['host'],
                pwd=details['pwd'])
        try:
            self._source.execute(connect)
            return 'direct'
        except:
            return 'odbc'

    def _exec_ddl(self, ddl):
        """
        Execute a ddl, taking care of the commit or clever_ddl options

        :param ddl:
            A ddl in a ``str``.
        """
        if self._commit:
            try:
                self._target.execute(ddl)
            except pyodbc.ProgrammingError as e:
                if (e.args[0] == '42601') and self._args['clever_ddls']:
                    logger.info('DDL already exists, skip: {0}'.format(ddl.split('\n', 1)[0]))
                else:
                    raise e

    def migrate_ddls(self, objects=[]):
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

        :param ddl:
        """
        ddls = self._get_ddls(objects)

        if ddls is None:
            logger.info('No DDLs to migrate found...')
            return

        logger.info('Migrating DDLs...')
        count = 0
        for ddl in ddls.split(';'):
            ddl = ddl.strip()

            # pyodbc or vertica statement hangs when executing ''
            if ddl == '':
                continue

            # do not bother with copying projections over
            if self._is_proj(ddl):
                continue

            # do we need to find the start of a sequence?
            if self._is_sequence(ddl):
                ddl = self._update_sequence_start(ddl)

            # Do we need to replace an IDENTITY by a sequence?
            new_seq = None
            if self._uses_identity(ddl):
                ddl, new_seq = self._replace_identity(ddl)

            # for display only: 1st line of statement, to display object name
            logger.warning(ddl.split('\n', 1)[0])

            count += 1
            self._exec_ddl(ddl)

            if new_seq is not None:
                create = 'CREATE SEQUENCE {schema}.{name} START WITH {start}'.format(
                    schema=new_seq['schema'],
                    name=new_seq['name'],
                    start=new_seq['start']
                    )
                logger.debug(create)

                count += 1
                self._exec_ddl(create)

                alter = "ALTER TABLE {schema}.{table} ALTER COLUMN {col} SET DEFAULT NEXTVAL('{schema}.{name}')".format(
                    schema=new_seq['schema'],
                    name=new_seq['name'],
                    table=new_seq['table'],
                    col=new_seq['col'])
                logger.debug(alter)

                count += 1
                self._exec_ddl(alter)
        wouldhavebeen = 'would have been (with --comit)'
        if self._commit:
            wouldhavebeen = ''
        logger.info('{0} DDLs {1} migrated'.format(count, wouldhavebeen))

    def migrate_data(self, objects):
        """
        """
        logger.warning('Starting migrating data.')

        con_type = self._connection_type()

        tables = self._get_table_list(self._source, objects)

        for table in tables:
            tname = '{s}.{t}'.format(s=table[0], t=table[1])
            logging.info('Exporting {0}'.format(tname))
            if con_type == 'direct':
                sql = 'EXPORT TO VERTICA stgdwh.{t} SELECT * FROM {t} LIMIT l'.format(t=tname, l=self._args.get('limit', 'ALL'))
                logger.warning(sql + '...')
                nbrows = 0
                if self._commit:
                    self._source.execute(sql)
                    nbrows = self._source.rowcount
                logger.warning('%s rows exported.' % nbrows)
            elif con_type == 'odbc':
                sql = 'SELECT * FROM {t} LIMIT {l}'.format(t=tname, l=self._args.get('limit', 'ALL'))
                logger.warning(sql)

                self._source.execute(sql)
                batch = None
                # cannot start batch if tartget DDL does not exists, which could be the case in dryrun
                if self._commit:
                    batch = VerticaBatch(
                        dsn=self._target_dsn,
                        table_name=table[0] + '.' + table[1],
                        truncate_table=self._args.get('truncate', False),
                    )
                if self._commit:
                    while True:
                        row = self._source.fetchone()
                        if row is None:
                            break
                        batch.insert_list(row)
                    batch.commit()
                else:
                    # let's try one fetch, to make sure sql is right
                    # but we cannot do anything with it
                    row = self._source.fetchone()

            else:
                raise VerticaMigratorError("Connection type from source to target not expected ('{0}').".format(con_type))

        wouldhavebeen = 'would have been (with --comit)'
        if self._commit:
            wouldhavebeen = ''
        logger.info('All data {0} exported.'.format(wouldhavebeen))

        if con_type == 'direct':
            self._source.execute('DISCONNECT stgdwh')
