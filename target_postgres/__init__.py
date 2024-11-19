#!/usr/bin/env python3

import argparse
import hashlib
import io
import json
import os
import sys
import copy
from datetime import datetime
from decimal import Decimal
from tempfile import mkstemp
from typing import Any, Dict, List

from joblib import Parallel, delayed, parallel_backend  # type: ignore
from jsonschema import Draft7Validator, FormatChecker  # type: ignore
import singer  # type: ignore

from target_postgres.db_sync import DbSync

LOGGER = singer.get_logger('target_postgres')

DEFAULT_BATCH_SIZE_ROWS = 100000
DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel


class RecordValidationException(Exception):
    """Exception to raise when record validation failed"""


class InvalidValidationOperationException(Exception):
    """Exception to raise when internal JSON schema validation process failed"""


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into
    double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = {'type': ['null', 'string'],
                                                                            'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = {'type': ['null', 'string'],
                                                                          'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = {'type': ['null', 'string']}

    return extended_schema_message


def add_metadata_values_to_record(record_message):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    """
    extended_record = record_message['record']
    extended_record['_sdc_extracted_at'] = record_message.get('time_extracted')
    extended_record['_sdc_batched_at'] = datetime.now().isoformat()
    extended_record['_sdc_deleted_at'] = record_message.get('record', {}).get('_sdc_deleted_at')

    return extended_record

class HGJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()

        return super().default(o)

def build_record_hash(record: dict) -> str:
    return hashlib.sha256(json.dumps(record, cls=HGJSONEncoder).encode()).hexdigest()

def emit_state(state: Dict[str, Any]) -> None:
    """Emit state message to standard output then it can be
    consumed by other components"""
    if not state:
        return

    try:
        line = json.dumps(state)
        LOGGER.debug('Emitting state %s', line)
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()
    except Exception as e:
        LOGGER.error(f"Error emitting state: {e}")


# pylint: disable=too-many-locals,too-many-branches,too-many-statements,invalid-name,consider-iterating-dictionary
def persist_lines(config: dict, lines: io.TextIOWrapper) -> None:
    """Read singer messages and process them line by line"""
    flushed_state: Dict[str, Dict[str, Any]] = {}
    schemas: Dict[str, Dict[str, Any]] = {}
    validators: Dict[str, Draft7Validator] = {}
    records_to_load: Dict[str, Dict[str, Any]] = {}
    row_count: Dict[str, int] = {}
    stream_to_sync: Dict[str, DbSync] = {}
    total_row_count: Dict[str, int] = {}

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error('Unable to parse:\n%s', line)
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            flushed_state = persist_record_line(config, flushed_state, schemas, validators, records_to_load, row_count, stream_to_sync, total_row_count, o)

        elif t == 'STATE':
            # Ignore STATE messages
            continue

        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            flushed_state = persist_schema_line(config, flushed_state, schemas, validators, records_to_load, row_count, stream_to_sync, total_row_count, o)

        elif t == 'ACTIVATE_VERSION':
            flushed_state = persist_activate_version_line(flushed_state)

        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    # If some bucket has records that need to be flushed but haven't reached batch size
    # then flush all buckets.
    if sum(row_count.values()) > 0:
        # flush all streams one last time, delete records if needed, reset counts and then emit current state
        flushed_state = flush_streams(records_to_load, row_count, stream_to_sync, config, flushed_state)

    # emit latest state
    if flushed_state:
        if 'bookmarks' in flushed_state:
            for stream in flushed_state['bookmarks']:
                bookmark = flushed_state['bookmarks'][stream]
                if isinstance(bookmark, dict) and bookmark:
                    flushed_state['bookmarks'][stream] = [copy.deepcopy(bookmark)]
                elif isinstance(bookmark, dict) and not bookmark:
                    flushed_state['bookmarks'][stream] = []
                if 'summary' in flushed_state:
                    if stream not in flushed_state['summary']:
                        flushed_state['summary'][stream] = {
                            "success": 0,
                            "fail": 0,
                            "existing": 0,
                            "updated": 0
                        }
    emit_state(copy.deepcopy(flushed_state))

def persist_activate_version_line(flushed_state):
    LOGGER.debug('ACTIVATE_VERSION message')

    # Initially set flushed state
    if 'bookmarks' not in flushed_state:
        # Create bookmark key if not exists
        flushed_state["bookmarks"] = {}
    if 'summary' not in flushed_state:
        flushed_state["summary"] = {}
    return flushed_state

def persist_schema_line(config, flushed_state, schemas, validators, records_to_load, row_count, stream_to_sync, total_row_count, o):
    key_properties: Dict[str, List[str]] = {}
    stream = o['stream']

    schemas[stream] = float_to_decimal(o['schema'])
    validators[stream] = Draft7Validator(schemas[stream], format_checker=FormatChecker())

    # flush records from previous stream SCHEMA
    if row_count.get(stream, 0) > 0:
        flushed_state = flush_streams(records_to_load, row_count, stream_to_sync, config, flushed_state)

        # emit latest encountered state
        emit_state(flushed_state)

    # key_properties key must be available in the SCHEMA message.
    if 'key_properties' not in o:
        raise Exception("key_properties field is required")

    # Log based and Incremental replications on tables with no Primary Key
    # cause duplicates when merging UPDATE events.
    # Stop loading data by default if no Primary Key.
    #
    # If you want to load tables with no Primary Key:
    #  1) Set ` 'primary_key_required': false ` in the target-postgres config.json
    #  or
    #  2) Use fastsync [postgres-to-postgres, mysql-to-postgres, etc.]
    if config.get('primary_key_required', False) and len(o['key_properties']) == 0:
        LOGGER.critical("Primary key is set to mandatory but not defined in the [%s] stream", stream)
        raise Exception("key_properties field is required")

    key_properties[stream] = o['key_properties']

    if not config.get('insertion_method_tables'):
        config['insertion_method_tables'] = []
    
    if config.get('add_metadata_columns') or config.get('hard_delete'):
        stream_to_sync[stream] = DbSync(config, add_metadata_columns_to_schema(o))
    else:
        stream_to_sync[stream] = DbSync(config, o)

    stream_to_sync[stream].create_schema_if_not_exists()
    stream_to_sync[stream].sync_table()

    row_count[stream] = 0
    total_row_count[stream] = 0
    return flushed_state

def persist_record_line(config, flushed_state, schemas, validators, records_to_load, row_count, stream_to_sync, total_row_count, o):
    batch_size_rows = config.get('batch_size_rows', DEFAULT_BATCH_SIZE_ROWS)
    if o['stream'] not in schemas:
        raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
    stream = o['stream']

            # Validate record
    if config.get('validate_records'):
        try:
            validators[stream].validate(float_to_decimal(o['record']))
        except Exception as ex:
            if type(ex).__name__ == "InvalidOperation":
                raise InvalidValidationOperationException(
                            f"Data validation failed and cannot load to destination. RECORD: {o['record']}\n"
                            "multipleOf validations that allows long precisions are not supported (i.e. with 15 digits"
                            "or more) Try removing 'multipleOf' methods from JSON schema.") from ex
            raise RecordValidationException(
                        f"Record does not pass schema validation. RECORD: {o['record']}") from ex

    primary_key_string = stream_to_sync[stream].record_primary_key_string(o['record'])
    if not primary_key_string:
        primary_key_string = 'RID-{}'.format(total_row_count[stream])

    if stream not in records_to_load:
        records_to_load[stream] = {}

            # increment row count only when a new PK is encountered in the current batch
    if primary_key_string not in records_to_load[stream]:
        row_count[stream] += 1
        total_row_count[stream] += 1

            # append record
    if config.get('add_metadata_columns') or config.get('hard_delete'):
        records_to_load[stream][primary_key_string] = add_metadata_values_to_record(o)
    else:
        records_to_load[stream][primary_key_string] = o['record']

    row_count[stream] = len(records_to_load[stream])

    if row_count[stream] >= batch_size_rows:
        # flush all streams, delete records if needed, reset counts and then emit current state
        if config.get('flush_all_streams'):
            filter_streams = None
        else:
            filter_streams = [stream]

        # Flush and return a new state dict with new positions only for the flushed streams
        flushed_state = flush_streams(records_to_load,
                                              row_count,
                                              stream_to_sync,
                                              config,
                                              flushed_state,
                                              filter_streams=filter_streams)

        # emit last encountered state
        emit_state(copy.deepcopy(flushed_state))
    return flushed_state


# pylint: disable=too-many-arguments
def flush_streams(
        streams: Dict[str, Dict[str, Any]],
        row_count: Dict[str, int],
        stream_to_sync: Dict[str, DbSync],
        config: Dict[str, Any],
        flushed_state: Dict[str, Any],
        filter_streams=None):
    """
    Flushes all buckets and resets records count to 0 as well as empties records to load list
    :param streams: dictionary with records to load per stream
    :param row_count: dictionary with row count per stream
    :param stream_to_sync: Postgres db sync instance per stream
    :param config: dictionary containing the configuration
    :param state: dictionary containing the original state from tap
    :param flushed_state: dictionary containing updated states only when streams got flushed
    :param filter_streams: Keys of streams to flush from the streams dict. Default is every stream
    :return: State dict with flushed positions
    """
    parallelism = config.get("parallelism", DEFAULT_PARALLELISM)
    max_parallelism = config.get("max_parallelism", DEFAULT_MAX_PARALLELISM)

    if 'bookmarks' not in flushed_state:
        # Create bookmark key if not exists
        flushed_state["bookmarks"] = {}
    if 'summary' not in flushed_state:
        flushed_state["summary"] = {stream: {
            "success": 0,
            "fail": 0,
            "existing": 0,
            "updated": 0
        } for stream in streams.keys()}

    # Parallelism 0 means auto parallelism:
    #
    # Auto parallelism trying to flush streams efficiently with auto defined number
    # of threads where the number of threads is the number of streams that need to
    # be loaded but it's not greater than the value of max_parallelism
    if parallelism == 0:
        n_streams_to_flush = len(streams.keys())
        if n_streams_to_flush > max_parallelism:
            parallelism = max_parallelism
        else:
            parallelism = n_streams_to_flush

    # Select the required streams to flush
    if filter_streams:
        streams_to_flush = filter_streams
    else:
        streams_to_flush = streams.keys()

    # Single-host, thread-based parallelism
    with parallel_backend('threading', n_jobs=parallelism):
        batch_states = {
            "bookmarks": {},
            "summary": {stream: {
                "success": 0,
                "fail": 0,
                "existing": 0,
                "updated": 0
            } for stream in streams.keys()}
        }

        results = Parallel()(delayed(load_stream_batch)(
            stream=stream,
            records_to_load=streams[stream],
            row_count=row_count,
            db_sync=stream_to_sync[stream],
            delete_rows=config.get('hard_delete'),
            temp_dir=config.get('temp_dir')
        ) for stream in streams_to_flush)

        for result in results:
            # Combine bookmarks
            for stream, bookmarks in result["bookmarks"].items():
                if stream not in batch_states["bookmarks"]:
                    batch_states["bookmarks"][stream] = []
                batch_states["bookmarks"][stream].extend(bookmarks)
                # Combine summary
                for key in batch_states["summary"][stream]:
                    batch_states["summary"][stream][key] += result["summary"][stream].get(key, 0)


    # reset flushed stream records to empty to avoid flushing same records
    for stream in streams_to_flush:
        streams[stream].clear()
    
    if len(streams_to_flush) > 0:
        for stream in streams_to_flush:
            if stream not in flushed_state['bookmarks']:
                flushed_state['bookmarks'][stream] = []
            elif isinstance(flushed_state['bookmarks'][stream], dict):
                flushed_state['bookmarks'][stream] = [copy.deepcopy(flushed_state['bookmarks'][stream])]
            flushed_state['bookmarks'][stream].extend(batch_states['bookmarks'][stream])
            for key in flushed_state['summary'][stream]:
                flushed_state['summary'][stream][key] += batch_states['summary'][stream][key]

    # Return with state message with flushed positions
    return flushed_state


# pylint: disable=too-many-arguments
def load_stream_batch(stream, records_to_load, row_count, db_sync:DbSync, delete_rows=False, temp_dir=None):
    """Load a batch of records and do post load operations, like creating
    or deleting rows"""
    # Load into Postgres
    batch_state = {
        "bookmarks": {},
        "summary": {
            "success": 0,
            "fail": 0,
            "existing": 0,
            "updated": 0
        }
    }
    try:
        if row_count[stream] > 0:
            batch_state = flush_records(stream, records_to_load, row_count[stream], db_sync, temp_dir)

        # Load finished, create indices if required
        db_sync.create_indices(stream)

        # Delete soft-deleted, flagged rows - where _sdc_deleted at is not null
        if delete_rows:
            db_sync.delete_rows(stream)

        # reset row count for the current stream
        row_count[stream] = 0
    except Exception as e:
        batch_state["summary"][stream]["fail"] += len(records_to_load)
        for record in records_to_load.values():
            bookmark = {
                "hash": build_record_hash(record),
                "success": False,
                "error": str(e)
            }
            if "externalId" in record:
                bookmark["external_id"] = record["externalId"]
            batch_state["bookmarks"][stream].append(bookmark)

    return batch_state
# pylint: disable=unused-argument
def flush_records(stream, records_to_load, row_count, db_sync: DbSync, temp_dir=None):
    """Take a list of records and load into database"""
    if temp_dir:
        temp_dir = os.path.expanduser(temp_dir)
        os.makedirs(temp_dir, exist_ok=True)

    size_bytes = 0
    csv_fd, csv_file = mkstemp(suffix='.csv', prefix=f'{stream}_', dir=temp_dir)
    batch_bookmarks = []
    batch_summary = {
        "success": 0,
        "fail": 0,
        "existing": 0,
        "updated": 0
    }
    with open(csv_fd, 'w+b') as f:
        for record in records_to_load.values():
            csv_line = db_sync.record_to_csv_line(record)
            bookmark = {
                "hash": build_record_hash(record),
                "success": True,
            }
            if "externalId" in record:
                bookmark["external_id"] = record["externalId"]

            batch_bookmarks.append(bookmark)
            f.write(bytes(csv_line + '\n', 'UTF-8'))

    size_bytes = os.path.getsize(csv_file)
    try:
        inserted_lines, updated_lines = db_sync.load_csv(csv_file, row_count, size_bytes)
        batch_summary["success"] += inserted_lines
        batch_summary["updated"] += updated_lines
    except Exception as e:
        for batch_bookmark in batch_bookmarks:
            batch_bookmark["error"] = str(e)
            batch_bookmark["success"] = False
        batch_summary["fail"] += len(batch_bookmarks)

    # Delete temp file
    os.remove(csv_file)
    return {
        "bookmarks": {stream: batch_bookmarks},
        "summary": {stream: batch_summary}
    }


def main():
    """Main entry point"""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', '--config', help='Config file')
    args = arg_parser.parse_args()

    if args.config:
        with open(args.config) as config_input:
            config = json.load(config_input)
    else:
        config = {}

    # Consume singer messages
    singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    persist_lines(config, singer_messages)

    LOGGER.debug("Exiting normally")


if __name__ == '__main__':
    main()
