"""Functions for keeping track whether an input file has already been 'processed' """

from datetime import datetime
from typing import Dict, Iterable, Tuple

import sqlalchemy
from sqlalchemy.orm import declarative_base

import mara_db.config
import mara_db.dbs

Base = declarative_base()


class ProcessedFile(Base):
    """A local file that has been 'processed' (e.g. has been read)"""
    __tablename__ = 'data_integration_processed_file'

    node_path = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Text), primary_key=True)
    file_name = sqlalchemy.Column(sqlalchemy.Text, primary_key=True)
    last_modified_timestamp = sqlalchemy.Column(sqlalchemy.TIMESTAMP(timezone=True))


def track_processed_file(node_path: str, file_name: str, last_modified_timestamp: datetime):
    """
    Records that a file has been 'processed' by a node

    Args:
        node_path: The path of the node that processed the file
        file_name: The name of the file that has been processed
        last_modified_timestamp: The time when the file was modified last

    Returns: True
    """
    with mara_db.dbs.cursor_context('mara') as cursor:
        cursor.execute(f'''
INSERT INTO data_integration_processed_file (node_path, file_name, last_modified_timestamp)
VALUES ({'%s,%s,%s'})
ON CONFLICT (node_path, file_name)
DO UPDATE SET last_modified_timestamp = EXCLUDED.last_modified_timestamp
''', (node_path, file_name, last_modified_timestamp))
    return True


def track_processed_files(node_path: str, files_with_timestamps: Iterable[Tuple[str, datetime]]):
    """
    Records that a batch of files has been 'processed' by a node in one go.

    In contrast to `track_processed_file`, an already stored timestamp is never
    moved backwards. That makes the call safe when several pipeline runs publish
    concurrently: a stale run that finishes after a fresher one cannot overwrite
    the fresher processing record with an older file modification timestamp.

    Args:
        node_path: The path of the node that processed the files
        files_with_timestamps: Pairs of (file name, last modification timestamp)

    Returns: True
    """
    files_with_timestamps = list(files_with_timestamps)
    if not files_with_timestamps:
        return True

    with mara_db.dbs.cursor_context('mara') as cursor:
        # executemany is required because psycopg2's execute does not accept a
        # sequence of parameter tuples for a single placeholder group
        cursor.executemany(f'''
INSERT INTO data_integration_processed_file (node_path, file_name, last_modified_timestamp)
VALUES ({'%s,%s,%s'})
ON CONFLICT (node_path, file_name)
DO UPDATE SET last_modified_timestamp = GREATEST(
    data_integration_processed_file.last_modified_timestamp,
    EXCLUDED.last_modified_timestamp)
''', [(node_path, file_name, last_modification_timestamp)
      for file_name, last_modification_timestamp in files_with_timestamps])
    return True


def already_processed_files(node_path: str) -> Dict[str, datetime]:
    """
    Returns all files that already have been processed by a node
    Args:
        node_path: The path of the node that processed the file

    Returns:
        A mapping of file names to timestamps of last modification
    """
    with mara_db.dbs.cursor_context('mara') as cursor:
        cursor.execute(f"""
SELECT file_name, last_modified_timestamp
FROM data_integration_processed_file WHERE node_path = {'%s'}
""", (node_path,))
        return {row[0]: row[1] for row in cursor.fetchall()}
