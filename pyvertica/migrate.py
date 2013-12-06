import datetime
import logging
import re
import subprocess
import sys
from subprocess import CalledProcessError
import time

import pyodbc

from pyvertica.connection import get_connection, connection_details
from pyvertica.batch import VerticaBatch

logger = logging.getLogger(__name__)


class VerticaMigratorError(Exception):
    """
    Error specific to the ``pyvertica.migrate`` module.
    """
    pass


class VerticaMigrator(object):
    """
    Completely copy over (minus the projections) a vertica database to
    another, including DDLs and data.

    :param source:
        A ``str`` being the source DSN.

    :param target:
        A ``str`` being the targetDSN.

    :param commit:
        A ``bool`` asking to commit or not the changes.

    :param args:
        A ``dict`` of extra parameters. It will contain the command-line
        arguments + the configuration values.

        .. seealso:: :ref:`vertica_migrate`.

    """

    # regexp to get name of the CREATE SEQUENCE statements
    _find_seq = re.compile(
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

    # Check if we are creating a temporary table
    _find_tmp_table = re.compile('^\s*CREATE TEMPORARY TABLE.*')

    def __init__(self, source, target, commit=False, **kwargs):
        logger.debug(
            'Initializing VerticaMigrator from {0} to {1}'.format(
                source, target))

        self._source_dsn = source
        self._target_dsn = target
        self._commit = commit
        self._kwargs = kwargs
        self._set_connections()

        self._sanity_checks()

    def _set_connections(self):
        """
        Setup db connections
        """

        ip_sql = (
            'SELECT node_address FROM v_catalog.nodes '
            'ORDER BY node_name LIMIT 1'
        )

        self._source_con = get_connection(
            dsn=self._source_dsn,
            user=self._kwargs.get('source_user'),
            password=self._kwargs.get('source_pwd'),
            reconnect=self._kwargs.get('source_reconnect', True),
        )
        self._source = self._source_con.cursor()
        self._source_ip = self._source.execute(ip_sql).fetchone()[0]

        self._target_con = get_connection(
            dsn=self._target_dsn,
            user=self._kwargs.get('target_user'),
            password=self._kwargs.get('target_pwd'),
            reconnect=self._kwargs.get('target_reconnect', True)
        )
        self._target = self._target_con.cursor()
        self._target_ip = self._target.execute(ip_sql).fetchone()[0]

    def _sanity_checks(self):
        """
        Make sure we are not doing something stupid like read and write the
        same database.
        """
        # copying from and to the same server is probably a bad idea, but let's
        # give the benefit of the doubt and check the DB
        if self._source_ip == self._target_ip:
            target_db = self._target.execute(
                'SELECT CURRENT_DATABASE').fetchone()[0]
            source_db = self._source.execute(
                'SELECT CURRENT_DATABASE').fetchone()[0]
            if target_db == source_db:
                raise VerticaMigratorError("Source and target database are "
                                           "the same. Will stop here.")
            else:
                logger.info('Copying inside the same server to another DB.')

        # let's not copy over a not empty database
        is_target_empty = self._target.execute("SELECT count(*) "
                                               "FROM tables WHERE "
                                               "is_system_table=false "
                                               "AND is_temp_table="
                                               "false").fetchone()[0]

        if is_target_empty > 0:
            if ('even_not_empty' in self._kwargs and
                    self._kwargs['even_not_empty']):
                logger.info('Target DB not empty but copy anyway.')
            else:
                raise VerticaMigratorError("Target vertica is not empty.")

    def _get_ddls(self, objects=[]):
        """
        Query the source vertica to get the DDLs as a big string, using the
        ``EXPORT_OBJECTS`` SQL function.

        It happens that this function returns ``None`` from odbc. In that case
        vsql is used, and the ``source_pwd`` parameter becomes useful.

        :return:
            A ``str`` containg the DDLs.
        """
        logger.info('Getting DDLs...')
        export_sql = "SELECT EXPORT_OBJECTS('', '{0}', False)".format(
            ','.join(objects))

        # I often have a segfault when running this, so let's fallback
        # by default
        # from_db = self._source.execute(export_sql).fetchone()
        from_db = None
        if from_db is None:
            logger.warning('Exporting object is done via vsql')
            details = connection_details(self._source_con)

            err = 'Unknown error'
            try:
                pr = subprocess.Popen([
                    '/opt/vertica/bin/vsql',
                    '-U', self._kwargs.get('source_user'),
                    '-h', self._kwargs.get('source_host'),
                    '-d', details['db'],
                    '-t',  # rows only
                    '-w',  self._kwargs.get('source_pwd'),
                    '-c',  export_sql
                ],  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

    def _is_temporary_table(self, ddl):
        """
        Is the ddl in parameter a ``CREATE TEMPORARY TABLE``?

        :param ddl:
            A ddl as a ``str``.

        :return:
            A ``bool`` ``True`` is yes, ``False`` otherwise.
        """
        m_tmps = self._find_tmp_table.search(ddl)
        return m_tmps is not None

    def _is_sequence(self, ddl):
        """
        Is the ddl in parameter a ``CREATE SEQUENCE``?

        :param ddl:
            A ddl as a ``str``.

        :return:
            A ``bool`` ``True` is yes, ``False`` otherwise.
        """
        m_seqs = self._find_seq.search(ddl)
        return m_seqs is not None

    def _update_sequence_start(self, ddl):
        """
        Called only if the ddl in parameters is a ``CREATE TABLE`` with
        ``SEQUENCE``. It updates the ``SEQUENCE`` to add a ``START`` value.

        :param ddl:
            A ddl as a ``str``.

        :return:
            A new ddl ``str``.
        """
        m_seqs = self._find_seq.search(ddl)
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
        Is the ddl in parameter a ``CREATE TABLE`` with an ``IDENTITY``?

        :param ddl:
            A ddl as a ``str``.

        :return:
            A ``bool`` True is yes, False otherwise.
        """
        m_ids = self._find_identity.search(ddl)
        return m_ids is not None

    def _replace_identity(self, ddl):
        """
        Called only if the ddl in parameters is a ``CREATE TABLE`` with
        ``IDENTITY``. It replaces the ``IDENTITY`` by a ``INT NOT NULL`` and
        defines a ``SEQUENCE`` to be added later.

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
        max_seq = self._source.execute('SELECT MAX({col})FROM {schema}.'
                                       '{table}'.format(schema=schema,
                                                        table=table,
                                                        col=col)).fetchone()[0]

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
        tables_sql = ("SELECT table_schema as s, table_name as t "
                      "FROM tables WHERE is_system_table=false AND "
                      "is_temp_table=false")
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
                    where.append(
                        "table_schema='{s}' AND table_name='{t}'".format(
                            t=table, s=schema))

        if len(where) > 0:
            tables_sql += ' AND ((' + ') OR ('.join(where) + '))'

        tret = con.execute(tables_sql).fetchall()
        return tret

    def _connection_type(self):
        """
        Finds out if the migration can be done directly via the ``EXPORT`` in
        Vertica, or if data needs to be loaded via odbc.

        :return:
            A ``str`` stating 'direct' or 'odbc'.
        """
        details = connection_details(self._target)

        connect = (
            "CONNECT TO VERTICA {db} USER {user} "
            "PASSWORD '{pwd}' ON '{host}',5433".format(
                db=details['db'],
                user=self._kwargs.get('target_user'),
                host=self._kwargs.get('target_host'),
                pwd=self._kwargs.get('target_pwd')
            )
        )

        try:
            self._source.execute(connect)
            self._target.execute(
                'CREATE GLOBAL TEMPORARY TABLE tmp_connect (test VARCHAR(42)) '
                'ON COMMIT DELETE ROWS'
            )
            self._source.execute(
                'EXPORT TO VERTICA {db}.tmp_connect AS SELECT * '
                'FROM v_catalog.dual'.format(
                    db=details['db']
                )
            )
            self._target.execute('DROP TABLE tmp_connect')

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
                # 42601: table, 42710: seq
                if (e.args[0] in ['42601', '42710'] and
                        self._kwargs['clever_ddls']):
                    logger.info('DDL already exists, skip: {0}'.format(
                        ddl.split('\n', 1)[0]))
                else:
                    raise e

    def migrate_ddls(self, objects=[]):
        """
        Migrates DDLs from the source to the target.
        Algo:
        - Get the full DDL from ``EXPORT_OBJECTS``
          - Split by ; to get statement one by one
          - If the statement is a ``CREATE PROJECTION``:
            - do nothing
          - If the statement is a ``CREATE TEMPORARY TABLE``:
            - do nothing
          - If the statement is a ``CREATE SEQUENCE``
             - find the current value which will thus be a ``START WITH``
          - If the statement is a ``CREATE TABLE`` which define an ``IDENTITY``
            - replace ``IDENTITY`` by ``INT NOT NULL``
            - remember schema, table, column, sequence name, current sequence
            - create a sequence based on this
            - alter table to use the sequence
          - execute each statement (only if commit)

        - in case of error, add the statement again to the list of DDLS
          - rerun while the list shrinks at each new iteration

        :param objects:
            A list of objects to migrates
        """
        objects = self._get_ddls(objects)

        if objects is None:
            logger.info('No DDLs to migrate found...')
            return

        logger.warning('Migrating DDLs...')
        count = 0
        ddls = objects.split(';')

        # If a ddl fails, it might be because dependent objects are not
        # migrated yet.
        # So in case of error, put them in errors, and stop when errors
        # does not shrink anymore.
        last_error = sys.maxint
        errors = []

        while len(ddls) >= 0:
            # we should have been done, but there were errors
            # Let's retry them
            if len(ddls) == 0 and len(errors) > 0:
                if len(errors) < last_error:
                    last_error = len(errors)
                    logging.warning(
                        '{nb} DDL migration errors, retrying them.'.format(
                            nb=last_error))
                    ddls.extend(errors)
                    errors = []
                else:
                    #error list is not shrinking
                    # display all of them
                    for e in errors:
                        try:
                            self._exec_ddl(e)
                        except Exception as e:
                            logging.exception(e)
                    raise VerticaMigratorError(
                        'Unrecoverable errors detected during DDL migration, '
                        'aborting'
                    )

            try:
                ddl = ddls.pop(0).strip()
            except IndexError:
                # we're done
                break

            # pyodbc or vertica statement hangs when executing ''
            if ddl == '':
                continue

            # do not bother with copying projections over
            if self._is_proj(ddl):
                continue

            # temporary tables will not be ported
            if self._is_temporary_table(ddl):
                continue

            # do we need to find the start of a sequence?
            if self._is_sequence(ddl):
                ddl = self._update_sequence_start(ddl)

            # Do we need to replace an IDENTITY by a sequence?
            new_seq = None
            if self._uses_identity(ddl):
                ddl, new_seq = self._replace_identity(ddl)

            # for display only: 1st line of statement, to display object name
            logger.info(ddl.split('\n', 1)[0])

            try:
                self._exec_ddl(ddl)
                count += 1
            except:
                errors.append(ddl)

            if new_seq is not None:
                create = ('CREATE SEQUENCE {schema}.{name} START WITH '
                          '{start}').format(schema=new_seq['schema'],
                                            name=new_seq['name'],
                                            start=new_seq['start'])
                logger.info(create)

                count += 1
                self._exec_ddl(create)

                alter = ("ALTER TABLE {schema}.{table} "
                         "ALTER COLUMN {col} "
                         "SET DEFAULT NEXTVAL('{schema}.{name}'"
                         ")").format(schema=new_seq['schema'],
                                     name=new_seq['name'],
                                     table=new_seq['table'],
                                     col=new_seq['col'])
                logger.info(alter)

                count += 1
                self._exec_ddl(alter)

        wouldhavebeen = 'would have been (with --commit)'
        if self._commit:
            wouldhavebeen = ''
        logger.warning('{0} DDLs {1} migrated'.format(count, wouldhavebeen))

    def _migrate_table(self, con_type, tname, target_details):
        """
        Migrate one table.

        :param con_type:
            Type of connection, ``str``. One of ``odbc`` or ``direct``.

        :param tname:
            ``str`` table name to migrate

        :target_details:
            ``dict`` of conenction deatils, returned from
            :func:`.connection_details`.

        """
        limit = self._kwargs.get('limit', 'ALL')
        sql = 'AT EPOCH LATEST SELECT * FROM {t} LIMIT {l}'.format(
            t=tname, l=limit)
        nbrows = 0

        if con_type == 'direct':
            sql = 'EXPORT TO VERTICA {db}.{t} AS {s}'.format(
                db=target_details['db'], t=tname, s=sql)

            if self._commit:
                if self._kwargs.get('truncate', False):
                    self._target.execute('TRUNCATE TABLE {t}'.format(t=tname))
                self._source.execute(sql)
                nbrows = self._source.rowcount
        elif con_type == 'odbc':
            self._source.execute(sql)
            batch = None

            # cannot start batch if target DDL does not exists,
            # which could be the case in dryrun
            if self._commit:
                batch = VerticaBatch(
                    odbc_kwargs={
                        'dsn': self._target_dsn,
                        'user': self._kwargs.get('target_user'),
                        'password': self._kwargs.get('target_pwd'),
                    },
                    table_name=tname,
                    truncate_table=self._kwargs.get('truncate', False),
                    reconnect=self._kwargs.get('target_reconnect', True),
                )
                while True:
                    row = self._source.fetchone()
                    if row is None:
                        break
                    batch.insert_list([
                        x.decode('utf-8')
                        if isinstance(x, str)
                        else x for x in row
                    ])
                    nbrows += 1
                batch.commit()
            else:
                # let's try one fetch, to make sure sql is right
                # but we cannot do anything with it
                row = self._source.fetchone()
        else:
            raise VerticaMigratorError(("Connection type from source"
                                        " to target not 'odbc' or 'direct'"
                                        " but: '{0}'.").format(con_type))

        logger.info('{nb} rows exported'.format(nb=nbrows))

    def migrate_data(self, objects):
        """
        Migrate data.

        :param objects:
            A ``list`` of objects to migrate.
        """
        logger.warning('Starting migrating data.')

        con_type = self._connection_type()
        logger.warning('Connection type: {t}'.format(t=con_type))

        # used if we are direct, cannot hurt otherwise, and save a lot of
        # queries if done now instead of inside _migrate_table
        target_details = connection_details(self._target)

        tables = self._get_table_list(self._source, objects)
        done_tbl = 0
        errors = []
        nbrows = 0

        try:
            while len(tables) > 0:
                table = tables.pop(0)
                tname = '{s}.{t}'.format(s=table[0], t=table[1])
                done_tbl += 1
                logging.info('Exporting data of {t}'.format(t=tname))

                try:
                    nbrows = self._migrate_table(
                        con_type, tname, target_details)
                except pyodbc.ProgrammingError as e:
                    errors.append(tname)
                    logger.error('Something went wrong during '
                                 'data copy for table {t}. Waiting 2 '
                                 'minutes to resume'.format(t=tname))
                    logger.error("{c}: {t}".format(c=e.args[0], t=e.args[1]))
                    # wait a few minutes in case the cluster comes back to life
                    time.sleep(120)

                logger.info('{d} tables done ({r} exportes), {td} todo'.format(
                    d=done_tbl, td=len(tables), r=nbrows))

        except Exception as e:
            logger.error('Something went very wrong during data copy '
                         'for table {t}.'.format(t=tname))
            errors.append(tname)
            for t in tables:
                errors.append('{s}.{t} '.format(s=t[0], t=t[1]))
            logger.error('Missing tables:')
            logger.error(' '.join(errors))
            # re-raise last exception
            raise

        wouldhavebeen = '' if self._commit else 'would have been with --commit'
        logger.warning('All data {0} exported.'.format(wouldhavebeen))

        if len(errors) > 0:
            logger.error('Missing tables:')
            logger.error(' '.join(errors))

        if con_type == 'direct':
            self._source.execute('DISCONNECT {db}'.format(
                db=target_details['db']))
