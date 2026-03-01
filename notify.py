"""Notification Dispatcher — Structured logging for webhook events.

Logs all Form Submission creates, opt-outs, and errors in a structured
JSON format for monitoring via webhook.log.
"""

import json
import logging

log = logging.getLogger('notify')


def send_notification(event_type, details):
    """Log a structured notification for a webhook event.

    Args:
        event_type: 'created', 'error', 'optout', 'duplicate', 'skip'
        details: dict with record details (contact_id, job_id, submission_id, etc.)
    """
    entry = {
        'event': event_type,
        'chat_id': details.get('chat_id', ''),
        'contact_id': details.get('contact_id', ''),
        'job_id': details.get('job_id', ''),
        'tier': details.get('tier', ''),
    }

    if event_type == 'created':
        entry['submission_id'] = details.get('submission_id', '')
        entry['task_id'] = details.get('task_id', '')
        entry['lead_outcome'] = details.get('lead_outcome', '')
        log.info(f"SF_CREATE | {json.dumps(entry)}")
    elif event_type == 'error':
        entry['error'] = details.get('error', '')
        log.error(f"SF_ERROR | {json.dumps(entry)}")
    elif event_type == 'optout':
        entry['optout_text'] = details.get('optout_text', '')[:50]
        log.info(f"OPT_OUT | {json.dumps(entry)}")
    else:
        log.info(f"EVENT | {json.dumps(entry)}")
