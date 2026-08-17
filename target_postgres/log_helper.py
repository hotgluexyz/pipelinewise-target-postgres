"""Shared structured logging helper used by target_postgres and db_sync."""
import json


def log_event(logger, event, *, level='info', **context):
    payload = {k: v for k, v in context.items() if v is not None}
    getattr(logger, level)("%s %s", event, json.dumps(payload, sort_keys=True, default=str))
