import datetime
import enum
import glob
import hashlib
import json
import math
import os.path
import pathlib
import re
import uuid
from html import escape
from typing import Dict, List, Tuple, Optional

import mara_db.config
import mara_db.dbs
from mara_page import _, html

from .. import config, pipelines
from ..commands import python, sql, files
from ..incremental_processing import file_dependencies as _file_dependencies
from ..incremental_processing import processed_files as _processed_files
from ..logging import logger


class ReadMode(enum.EnumMeta):
    """A mode for specifying which files from a list of files to load"""
    ALL = 'all'  # load all files that match the pattern
    ONLY_LATEST = 'only_latest'  # load only the latest matching file (requires date-regex)
    ONLY_NEW = 'only_new'  # load only files that have not been loaded yet
    ONLY_CHANGED = 'only_changed'  # load all files that were modified since the last run (or that are new)
    ONLY_NEW_EXCEPT_LATEST = 'only_new_except_latest'  # load only files that have not been loaded yet and not the last one


# Name of the table in the target database that records which day partitions have
# already been published by which node with which input. It guards against stale
# runs overwriting newer results when pipeline runs overlap.
DAY_LOAD_STATUS_TABLE = 'data_integration_day_partition_load'


class _ParallelRead(pipelines.ParallelTask):
    def __init__(self, id: str, description: str, file_pattern: str, read_mode: ReadMode, target_table: str,
                 max_number_of_parallel_tasks: Optional[int] = None, file_dependencies: Optional[List[str]] = None, date_regex: Optional[str] = None,
                 partition_target_table_by_day_id: bool = False, truncate_partitions: bool = False,
                 commands_before: Optional[List[pipelines.Command]] = None, commands_after: Optional[List[pipelines.Command]] = None,
                 db_alias: Optional[str] = None, timezone: Optional[str] = None,
                 max_retries: Optional[int] = None) -> None:
        pipelines.ParallelTask.__init__(self, id=id, description=description,
                                        max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                                        commands_before=commands_before, commands_after=commands_after,
                                        max_retries=max_retries)
        self.file_pattern = file_pattern
        self.read_mode = read_mode
        self.date_regex = date_regex
        self.file_dependencies = file_dependencies or []
        self.partition_target_table_by_day_id = partition_target_table_by_day_id
        if self.partition_target_table_by_day_id:
            assert date_regex

        self.truncate_partitions = truncate_partitions
        if self.truncate_partitions:
            assert partition_target_table_by_day_id

        self.target_table = target_table
        self._db_alias = db_alias
        self.timezone = timezone

    @property
    def db_alias(self):
        return self._db_alias or config.default_db_alias()

    def add_parallel_tasks(self, sub_pipeline: 'pipelines.Pipeline') -> None:
        import more_itertools

        files = []  # A list of (file_name, date_or_file_name) tuples
        data_dir = config.data_dir()
        first_date = config.first_date()

        for file in glob.iglob(str(pathlib.Path(data_dir, self.file_pattern))):
            file = str(pathlib.Path(file).relative_to(pathlib.Path(data_dir)))
            if self.date_regex:
                match = re.match(self.date_regex, file)
                if not match:
                    raise Exception(f'file name "{file}" \ndoes not match date regex "{self.date_regex}"')
                date = datetime.date(*[int(group) for group in match.groups()])
                if date >= first_date:
                    files.append((file, date))
            else:
                files.append((file, file))

        # sort by date when regex provided or by filename otherwise
        files.sort(key=lambda x: x[1], reverse=True)

        # remove latest file when requested
        if self.read_mode == ReadMode.ONLY_NEW_EXCEPT_LATEST:
            files = files[1:]

        # take only latest file when requested
        if files and len(files) > 0 and self.read_mode == ReadMode.ONLY_LATEST:
            files = files[:1]

        # remember the full set of existing files before incremental filtering;
        # needed for reloading complete day partitions
        all_existing_files = list(files)

        # for incremental loading, determine which files already have been processed
        # reprocess all when file dependencies changed
        if (self.read_mode not in (ReadMode.ALL, ReadMode.ONLY_LATEST)
                and (not self.file_dependencies
                     or not _file_dependencies.is_modified(self.path(), 'ParallelReadFile', self.parent.base_path(),
                                                           self.file_dependencies))):
            processed_files = _processed_files.already_processed_files(self.path())

            # files that are new or (for ONLY_CHANGED) modified since the last run
            files = [x for x in files
                     if x[0] not in processed_files  # everything not yet read
                     or (self.read_mode == ReadMode.ONLY_CHANGED  # everything modified
                         and self._last_modification_timestamp(x[0]) > processed_files[x[0]])]

        if not files:
            logger.log('No newer files', format=logger.Format.ITALICS)
            return

        changed_files = files

        if self.read_mode != ReadMode.ALL and self.file_dependencies:
            def update_file_dependencies():
                _file_dependencies.update(self.path(), 'ParallelReadFile', self.parent.base_path(),
                                          self.file_dependencies)
                return True

            sub_pipeline.final_node.commands.append(python.RunFunction(update_file_dependencies))

        chunk_size = math.ceil(len(changed_files) / (2 * config.max_number_of_parallel_tasks()))

        if self.partition_target_table_by_day_id:
            if not isinstance(mara_db.dbs.db(self.db_alias), mara_db.dbs.PostgreSQLDB):
                raise NotImplementedError(
                    f'Partitioning by day_id has only been implemented for postgresql so far, \n'
                    f'not for {mara_db.dbs.db(self.db_alias)}')

            if self.truncate_partitions:
                self._add_truncating_partition_tasks(sub_pipeline, changed_files, all_existing_files, chunk_size)
            else:
                self._add_appending_partition_tasks(sub_pipeline, changed_files, chunk_size)
        else:
            for n, chunk in enumerate(more_itertools.chunked(changed_files, chunk_size)):
                sub_pipeline.add(
                    pipelines.Task(id=str(n), description=f'Reads {len(chunk)} files',
                                   commands=sum([self.parallel_commands(x[0]) for x in chunk], []),
                                   max_retries=self.max_retries))

    def _add_appending_partition_tasks(self, sub_pipeline: 'pipelines.Pipeline',
                                       files: List[Tuple[str, datetime.date]], chunk_size: int) -> None:
        """The legacy partition path: create day partitions and append all selected files to them."""
        import more_itertools

        files_per_day = {}
        for (file, date) in files:
            files_per_day.setdefault(date, []).append(file)

        sql_statements = []
        for date in files_per_day.keys():
            sql_statements.append(f'CREATE TABLE IF NOT EXISTS {self._partition_table(date)}'
                                  + f' PARTITION OF {self.target_table} FOR VALUES IN ({self._day_id(date)});')

        create_partitions_task = pipelines.Task(
            id='create_partitions',
            description='Creates required target table partitions',
            commands=[sql.ExecuteSQL(sql_statement='\n'.join(slice), echo_queries=False, db_alias=self.db_alias)
                      for slice in more_itertools.sliced(sql_statements, 50)],
            max_retries=self.max_retries)

        sub_pipeline.add(create_partitions_task)

        for n, chunk in enumerate(more_itertools.chunked(files_per_day.items(), chunk_size)):
            task = pipelines.Task(id=str(n), description='Reads a portion of the files',
                                  max_retries=self.max_retries)
            for (day, files_of_day) in chunk:
                target_table = self._partition_table(day)
                for file in files_of_day:
                    task.add_commands(self.parallel_commands(file))
                task.add_command(sql.ExecuteSQL(sql_statement=f'ANALYZE {target_table}'))
            sub_pipeline.add(task, ['create_partitions'])

    def _add_truncating_partition_tasks(self, sub_pipeline: 'pipelines.Pipeline',
                                        changed_files: List[Tuple[str, datetime.date]],
                                        all_existing_files: List[Tuple[str, datetime.date]],
                                        chunk_size: int) -> None:
        """
        Builds an atomic day-partition reload chain.

        Any changed/new file makes its whole day "affected". Every file of an
        affected day is loaded into a private staging table (the published day
        partition stays untouched and readable), then all days are published in
        one final task: each day is replaced transactionally, the day load
        manifest is updated and the processed-file records are written only
        after the publish succeeds.
        """
        import more_itertools

        # 1. Fix the full input set of this run by the affected days.
        #    This ensures that truncating a day partition can never drop data of
        #    another, unchanged file of the same day.
        affected_days = sorted({date for (_file, date) in changed_files})
        files_per_day: Dict[datetime.date, List[str]] = {date: [] for date in affected_days}
        for (file, date) in all_existing_files:
            if date in files_per_day:
                files_per_day[date].append(file)

        # capture the input timestamps once; the same snapshot is used for
        # loading, staleness checks and the processed-file bookkeeping
        mtimes_per_day: Dict[datetime.date, Dict[str, datetime.datetime]] = {
            date: {file: self._last_modification_timestamp(file) for file in files_of_day}
            for date, files_of_day in files_per_day.items()}

        # a token unique per pipeline launch keeps staging tables of concurrent
        # runs apart (Date.now/uuid is fine here: this runs in a forked process,
        # not inside a workflow script)
        run_token = uuid.uuid4().hex[:8] + format(os.getpid(), 'x')
        staging_tables = {date: self._staging_table(date, run_token) for date in affected_days}

        # 2. Create day partitions (never truncate them here) and empty,
        #    private staging tables plus the manifest table.
        create_statements = [
            f'CREATE TABLE IF NOT EXISTS {self._partition_table(date)}'
            f' PARTITION OF {self.target_table} FOR VALUES IN ({self._day_id(date)});'
            for date in affected_days]
        create_statements.append(self._create_manifest_table_sql())
        for date in affected_days:
            # drop leftovers from an earlier, aborted attempt of *this* run, then
            # recreate the staging table with the structure of the day partition
            create_statements.append(f'DROP TABLE IF EXISTS {staging_tables[date]};')
            create_statements.append(
                f'CREATE TABLE {staging_tables[date]} '
                f'(LIKE {self._partition_table(date)} INCLUDING DEFAULTS);')

        # 2b. Claim each affected day for *this* input now, at planning time.
        #     A later run with newer input records a newer claim, so a run that
        #     planned early with stale input can never overwrite the newer
        #     result even if it reaches the publish lock first.
        node_path = self.path()
        publish_plan = [
            {'day_id': self._day_id(date),
             'partition_table': self._partition_table(date),
             'staging_table': staging_tables[date],
             'files': files_per_day[date],
             'input_hash': self._day_input_hash(mtimes_per_day[date]),
             # timestamp of the newest input file, fixed at planning time
             'max_mtime': max(mtimes_per_day[date].values())}
            for date in affected_days]
        for entry in publish_plan:
            create_statements.append(self._claim_day_sql(node_path, entry))

        create_partitions_task = pipelines.Task(
            id='create_partitions',
            description='Creates required target table partitions, staging tables and day claims',
            commands=[sql.ExecuteSQL(sql_statement='\n'.join(slice), echo_queries=False, db_alias=self.db_alias)
                      for slice in more_itertools.sliced(create_statements, 50)],
            max_retries=self.max_retries)
        sub_pipeline.add(create_partitions_task)

        # 3. Load the fixed full input of every affected day into its staging table
        for n, chunk in enumerate(more_itertools.chunked(affected_days, chunk_size)):
            task = pipelines.Task(id=str(n), description='Reads a portion of the files into staging tables',
                                  max_retries=self.max_retries)
            for day in chunk:
                staging_table = staging_tables[day]
                for file in files_per_day[day]:
                    # bookkeeping happens once in the publish task, not per file,
                    # so that failed loads leave no "processed" traces
                    task.add_commands(
                        self.parallel_commands(file, target_table_override=staging_table,
                                               track_processed=False))
                task.add_command(sql.ExecuteSQL(sql_statement=f'ANALYZE {staging_table}'))
            sub_pipeline.add(task, ['create_partitions'])

        load_task_ids = [node_id for node_id, node in sub_pipeline.nodes.items()
                         if node_id != 'create_partitions']

        # 4. Publish once, after every load succeeded.
        publish_task = pipelines.Task(
            id='publish_partitions',
            description='Atomically replaces day partitions and updates processing records',
            max_retries=self.max_retries)

        for entry in publish_plan:
            # each day is swapped in its own transaction; the old partition
            # remains fully readable until (and if) the transaction commits
            publish_task.add_command(sql.ExecuteSQL(
                sql_statement=self._publish_day_sql(node_path, entry),
                echo_queries=False, db_alias=self.db_alias))

        # ReadMode.ALL never writes processed-file records (the whole input is
        # reloaded every run); all other modes publish their bookkeeping once.
        if self.read_mode != ReadMode.ALL:
            def update_processing_records():
                # runs only when all publish transactions committed -> reruns do
                # not duplicate bookkeeping and failed loads leave no traces
                all_files = [(file, mtime)
                             for date in affected_days
                             for file, mtime in mtimes_per_day[date].items()]
                _processed_files.track_processed_files(node_path, all_files)
                return True

            publish_task.add_command(python.RunFunction(function=update_processing_records))
        sub_pipeline.add(publish_task, load_task_ids)

    def _claim_day_sql(self, node_path: List[str], entry: Dict) -> str:
        """
        Records the freshness of this run's input for a day at planning time.

        Uses the same advisory lock as publishing so concurrent claims are
        serialized. Only advances the claimed freshness; a run planning early
        with stale input cannot erase a newer claim made by a concurrent run.
        Each statement runs autocommitted (separate psql invocation), hence the
        explicit transaction wrapper around the advisory lock.
        """
        node_path_literal = self._sql_string_literal('/'.join(node_path))
        input_hash_literal = self._sql_string_literal(entry['input_hash'])
        max_mtime_literal = self._sql_string_literal(entry['max_mtime'].isoformat())
        lock_key = self._advisory_lock_key(node_path, entry['day_id'])
        return f"""
BEGIN;
SELECT pg_advisory_xact_lock({lock_key});
INSERT INTO {DAY_LOAD_STATUS_TABLE}
    (node_path, day_id, input_hash, max_input_modified_at,
     published_input_hash, published_max_input_modified_at, claimed_at, loaded_at)
VALUES ({node_path_literal}, {entry['day_id']}, {input_hash_literal},
        TIMESTAMPTZ {max_mtime_literal}, NULL, NULL, now(), now())
ON CONFLICT (node_path, day_id) DO UPDATE
SET input_hash = CASE
        WHEN EXCLUDED.max_input_modified_at
             > {DAY_LOAD_STATUS_TABLE}.max_input_modified_at
        THEN EXCLUDED.input_hash
        ELSE {DAY_LOAD_STATUS_TABLE}.input_hash END,
    max_input_modified_at = GREATEST(
        {DAY_LOAD_STATUS_TABLE}.max_input_modified_at,
        EXCLUDED.max_input_modified_at),
    claimed_at = now();
COMMIT;
"""

    def _publish_day_sql(self, node_path: List[str], entry: Dict) -> str:
        """
        Transactional, concurrency-safe replacement of one day partition.

        A transaction-scoped advisory lock keyed on (node, day) serializes all
        publishers. Under that lock the run's claimed freshness is compared with
        both the newest claim and the already published input: a stale run is
        rejected and leaves the existing partition untouched, so an old input can
        never overwrite a newer result. The input hash makes identical re-runs
        idempotent; the max input modification timestamp decides freshness.
        """
        node_path_literal = self._sql_string_literal('/'.join(node_path))
        input_hash_literal = self._sql_string_literal(entry['input_hash'])
        max_mtime_literal = self._sql_string_literal(entry['max_mtime'].isoformat())
        # stable 64-bit key for the advisory lock, independent of table contents
        lock_key = self._advisory_lock_key(node_path, entry['day_id'])
        return f"""
BEGIN;
-- serialize all publishers of this (node, day); released automatically at COMMIT/ROLLBACK
SELECT pg_advisory_xact_lock({lock_key});

DO $publish_day$
DECLARE
    current_claim_hash TEXT;
    current_claim_mtime TIMESTAMPTZ;
    published_hash TEXT;
    published_mtime TIMESTAMPTZ;
BEGIN
    SELECT input_hash, max_input_modified_at,
           published_input_hash, published_max_input_modified_at
      INTO current_claim_hash, current_claim_mtime,
           published_hash, published_mtime
    FROM {DAY_LOAD_STATUS_TABLE}
    WHERE node_path = {node_path_literal} AND day_id = {entry['day_id']};

    -- a concurrent run already claimed a newer input, or a newer input was
    -- published before; either way this stale input must not replace it
    IF (current_claim_hash IS DISTINCT FROM {input_hash_literal}
        AND current_claim_mtime > TIMESTAMPTZ {max_mtime_literal})
       OR (published_hash IS NOT NULL
        AND published_hash IS DISTINCT FROM {input_hash_literal}
        AND published_mtime > TIMESTAMPTZ {max_mtime_literal}) THEN
        RAISE EXCEPTION 'Refusing to publish day %: a newer input has already been claimed/published than this run''s input (%)',
            {entry['day_id']}, TIMESTAMPTZ {max_mtime_literal};
    END IF;

    TRUNCATE {entry['partition_table']};
    INSERT INTO {entry['partition_table']}
    SELECT * FROM {entry['staging_table']};

    UPDATE {DAY_LOAD_STATUS_TABLE}
    SET published_input_hash = {input_hash_literal},
        published_max_input_modified_at = TIMESTAMPTZ {max_mtime_literal},
        loaded_at = now()
    WHERE node_path = {node_path_literal} AND day_id = {entry['day_id']};
END
$publish_day$;

COMMIT;

ANALYZE {entry['partition_table']};
DROP TABLE IF EXISTS {entry['staging_table']};
"""

    @staticmethod
    def _advisory_lock_key(node_path: List[str], day_id: str) -> str:
        """Deterministic signed 64-bit key for pg_advisory_xact_lock."""
        digest = hashlib.sha256(('/'.join(node_path) + '|' + str(day_id)).encode('utf-8')).digest()
        value = int.from_bytes(digest[:8], byteorder='big', signed=True)
        # map into the signed bigint range PostgreSQL uses for advisory locks
        if value >= 2 ** 63:
            value -= 2 ** 64
        return str(value)

    def _create_manifest_table_sql(self) -> str:
        return f"""
CREATE TABLE IF NOT EXISTS {DAY_LOAD_STATUS_TABLE} (
    node_path TEXT NOT NULL,
    day_id BIGINT NOT NULL,
    -- freshness of the newest input claimed by any (possibly still running) load
    input_hash TEXT NOT NULL,
    max_input_modified_at TIMESTAMPTZ NOT NULL,
    -- freshness of the input that currently backs the published partition
    published_input_hash TEXT,
    published_max_input_modified_at TIMESTAMPTZ,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (node_path, day_id)
);"""

    @staticmethod
    def _sql_string_literal(value: str) -> str:
        """Quotes a python string as a PostgreSQL string literal (SQL-injection safe)."""
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _day_id(date: datetime.date) -> str:
        return date.strftime('%Y%m%d')

    def _partition_table(self, date: datetime.date) -> str:
        return f'{self.target_table}_{self._day_id(date)}'

    def _staging_table(self, date: datetime.date, run_token: str) -> str:
        # keep the name well below PostgreSQL's 63 char identifier limit even
        # when the target table has a long (schema-qualified) name
        return f'{self.target_table}__stg_{self._day_id(date)}_{run_token}'

    @staticmethod
    def _day_input_hash(mtimes: Dict[str, datetime.datetime]) -> str:
        """
        A deterministic fingerprint of a day's input: file names, their sizes and
        modification timestamps. Changes whenever any input file is replaced.
        """
        digest = hashlib.sha256()
        for file_name in sorted(mtimes.keys()):
            file_path = pathlib.Path(config.data_dir()) / file_name
            stat = os.stat(file_path)
            digest.update(file_name.encode('utf-8'))
            digest.update(b'\0')
            digest.update(str(stat.st_size).encode('ascii'))
            digest.update(b'\0')
            digest.update(mtimes[file_name].isoformat().encode('ascii'))
            digest.update(b'\n')
        return digest.hexdigest()

    def parallel_commands(self, file_name: str, target_table_override: Optional[str] = None,
                          track_processed: bool = True) -> List[pipelines.Command]:
        read_command = self.read_command(file_name)
        if target_table_override is not None:
            read_command.target_table = target_table_override
        return [read_command] + (
            [python.RunFunction(function=lambda file_name=file_name: _processed_files.track_processed_file(
                self.path(), file_name, self._last_modification_timestamp(file_name)))]
            if track_processed and self.read_mode != ReadMode.ALL else [])

    def read_command(self) -> pipelines.Command:
        raise NotImplementedError

    def _last_modification_timestamp(self, file_name):
        return datetime.datetime.fromtimestamp(
            os.path.getmtime(pathlib.Path(config.data_dir()) / file_name)).astimezone()


class ParallelReadFile(_ParallelRead):
    def __init__(self, id: str, description: str, file_pattern: str, read_mode: ReadMode,
                 compression: files.Compression, target_table: str, file_dependencies: Optional[List[str]] = None,
                 date_regex: Optional[str] = None, partition_target_table_by_day_id: bool = False,
                 truncate_partitions: bool = False,
                 commands_before: Optional[List[pipelines.Command]] = None, commands_after: Optional[List[pipelines.Command]] = None,
                 mapper_script_file_name: Optional[str] = None, make_unique: bool = False, db_alias: Optional[str] = None,
                 delimiter_char: Optional[str] = None, quote_char: Optional[str] = None, null_value_string: Optional[str] = None,
                 skip_header: Optional[bool] = None, csv_format: bool = False,
                 timezone: Optional[str] = None, max_number_of_parallel_tasks: Optional[int] = None,
                 max_retries: Optional[int] = None) -> None:
        _ParallelRead.__init__(self, id=id, description=description, file_pattern=file_pattern,
                               read_mode=read_mode, target_table=target_table, file_dependencies=file_dependencies,
                               date_regex=date_regex, partition_target_table_by_day_id=partition_target_table_by_day_id,
                               truncate_partitions=truncate_partitions,
                               commands_before=commands_before, commands_after=commands_after,
                               db_alias=db_alias, timezone=timezone,
                               max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                               max_retries=max_retries)
        self.compression = compression
        self.mapper_script_file_name = mapper_script_file_name or ''
        self.make_unique = make_unique
        self.delimiter_char = delimiter_char
        self.quote_char = quote_char
        self.skip_header = skip_header
        self.csv_format = csv_format
        self.null_value_string = null_value_string

    def read_command(self, file_name: str) -> pipelines.Command:
        return files.ReadFile(file_name=file_name, compression=self.compression, target_table=self.target_table,
                              mapper_script_file_name=self.mapper_script_file_name, make_unique=self.make_unique,
                              db_alias=self.db_alias, delimiter_char=self.delimiter_char, skip_header=self.skip_header,
                              quote_char=self.quote_char, null_value_string=self.null_value_string,
                              csv_format=self.csv_format, timezone=self.timezone)

    def html_doc_items(self) -> List[Tuple[str, str]]:
        path = self.parent.base_path() / self.mapper_script_file_name if self.mapper_script_file_name else pathlib.Path()
        return [('file pattern', _.i[self.file_pattern]),
                ('compression', _.tt[self.compression]),
                ('read mode', _.tt[self.read_mode]),
                ('date regex', _.tt[escape(self.date_regex)] if self.date_regex else None),
                ('file dependencies', [_.i[dependency, _.br] for dependency in self.file_dependencies]),
                ('mapper script file name', _.i[self.mapper_script_file_name]),
                (_.i['mapper script'], html.highlight_syntax(path.read_text().strip('\n')
                                                             if self.mapper_script_file_name and path.exists()
                                                             else '', 'python')),
                ('make unique', _.tt[repr(self.make_unique)]),
                ('skip header', _.tt[self.skip_header]),
                ('target_table', _.tt[self.target_table]),
                ('db alias', _.tt[self.db_alias]),
                ('partion target table by day_id', _.tt[self.partition_target_table_by_day_id]),
                ('truncate partitions', _.tt[self.truncate_partitions]),
                ('sql delimiter char',
                 _.tt[json.dumps(self.delimiter_char) if self.delimiter_char != None else None]),
                ('quote char', _.tt[json.dumps(self.quote_char) if self.quote_char != None else None]),
                ('null value string',
                 _.tt[json.dumps(self.null_value_string) if self.null_value_string != None else None]),
                ('time zone', _.tt[self.timezone])]


class ParallelReadSqlite(_ParallelRead):
    def __init__(self, id: str, description: str, file_pattern: str, read_mode: ReadMode, sql_file_name: str,
                 target_table: str, file_dependencies: Optional[List[str]] = None, date_regex: Optional[str] = None,
                 partition_target_table_by_day_id: bool = False, truncate_partitions: bool = False,
                 commands_before: Optional[List[pipelines.Command]] = None, commands_after: Optional[List[pipelines.Command]] = None,
                 db_alias: Optional[str] = None, timezone=None, max_number_of_parallel_tasks: Optional[int] = None,
                 max_retries: Optional[int] = None) -> None:
        _ParallelRead.__init__(self, id=id, description=description, file_pattern=file_pattern,
                               read_mode=read_mode, target_table=target_table, file_dependencies=file_dependencies,
                               date_regex=date_regex, partition_target_table_by_day_id=partition_target_table_by_day_id,
                               truncate_partitions=truncate_partitions,
                               commands_before=commands_before, commands_after=commands_after, db_alias=db_alias,
                               timezone=timezone, max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                               max_retries=max_retries)
        self.sql_file_name = sql_file_name

    def read_command(self, file_name: str) -> List[pipelines.Command]:
        return files.ReadSQLite(sqlite_file_name=file_name, sql_file_name=self.sql_file_name,
                                target_table=self.target_table, db_alias=self.db_alias, timezone=self.timezone)

    def sql_file_path(self):
        return self.parent.base_path() / self.sql_file_name

    def html_doc_items(self) -> List[Tuple[str, str]]:
        path = self.sql_file_path()
        return [('file pattern', _.i[self.file_pattern]),
                ('read mode', _.tt[self.read_mode]),
                ('date regex', _.tt[escape(self.date_regex)] if self.date_regex else None),
                ('file dependencies', [_.i[dependency, _.br] for dependency in self.file_dependencies]),
                ('query file name', _.i[self.sql_file_name]),
                (_.i['query'], html.highlight_syntax(path.read_text().strip('\n')
                                                     if self.sql_file_name and path.exists()
                                                     else '', 'sql')),
                ('target_table', _.tt[self.target_table]),
                ('db alias', _.tt[self.db_alias]),
                ('partion target table by day_id', _.tt[self.partition_target_table_by_day_id]),
                ('truncate partitions', _.tt[self.truncate_partitions]),
                ('time zone', _.tt[self.timezone])]
