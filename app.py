"""Violet Webhook Service v2 — Real-time RetellAI -> Salesforce lead handoff.

Receives custom tool calls from RetellAI Violet agent for real-time processing,
plus chat_analyzed webhooks as a fallback safety net.

Routes:
  POST /webhook/retell/tool  — Handle custom tool calls (real-time)
  POST /webhook/retell       — Handle chat_analyzed events (fallback)
  POST /webhook/sf/apply-now — Handle Apply Now form submissions from SF trigger
  GET  /health               — Health check (SF connection, uptime)
  GET  /status               — HTML monitoring dashboard
  POST /api/retry-failed     — Replay dead letter queue
"""

import hashlib
import hmac
import json
import logging
import os
import sys
import time
import threading
from datetime import datetime, timezone

from flask import Flask, request, jsonify, render_template

import violet_core
import dead_letter
from notify import send_notification
from salesforce_client import get_salesforce_credentials

# ══════════════════════════════════════════════════════════════════════
# APP SETUP
# ══════════════════════════════════════════════════════════════════════
app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('webhook.log', encoding='utf-8'),
    ],
)
log = logging.getLogger('app')

# ══════════════════════════════════════════════════════════════════════
# IN-MEMORY STATS (reset on restart — fine at this volume)
# ══════════════════════════════════════════════════════════════════════
_stats_lock = threading.Lock()
_stats = {
    'start_time': datetime.now(timezone.utc).isoformat(),
    'webhooks_received': 0,
    'tool_calls_received': 0,
    'created': 0,
    'duplicates': 0,
    'enriched': 0,
    'skipped': 0,
    'optouts': 0,
    'first_responses': 0,
    'apply_now_received': 0,
    'apply_now_sent': 0,
    'errors': 0,
    'last_webhook': None,
    'last_tool_call': None,
    'last_created': None,
    'recent_events': [],  # Last 50 events
}

RETELL_API_KEY = os.environ.get('RETELL_API_KEY', '')
MAX_RECENT_EVENTS = 50

# Debug: store last raw tool payload for diagnostics
_last_tool_payload = {}


def _record_event(event_type, chat_id, detail, source='webhook'):
    """Thread-safe stats update."""
    with _stats_lock:
        if source == 'tool':
            _stats['tool_calls_received'] += 1
            _stats['last_tool_call'] = datetime.now(timezone.utc).isoformat()
        else:
            _stats['webhooks_received'] += 1
            _stats['last_webhook'] = datetime.now(timezone.utc).isoformat()

        if event_type == 'created':
            _stats['created'] += 1
            _stats['last_created'] = datetime.now(timezone.utc).isoformat()
        elif event_type == 'duplicate':
            _stats['duplicates'] += 1
        elif event_type in ('skip', 'noted'):
            _stats['skipped'] += 1
        elif event_type == 'error':
            _stats['errors'] += 1
        elif event_type == 'enriched':
            _stats['enriched'] += 1
        elif event_type == 'optout':
            _stats['optouts'] += 1
        elif event_type == 'first_response':
            _stats['first_responses'] += 1
        elif event_type == 'apply_now_received':
            _stats['apply_now_received'] += 1
        elif event_type == 'apply_now_sent':
            _stats['apply_now_sent'] += 1

        _stats['recent_events'].append({
            'time': datetime.now(timezone.utc).strftime('%H:%M:%S'),
            'type': event_type,
            'source': source,
            'chat_id': chat_id[:12] + '...' if len(chat_id) > 12 else chat_id,
            'detail': str(detail)[:120],
        })
        if len(_stats['recent_events']) > MAX_RECENT_EVENTS:
            _stats['recent_events'] = _stats['recent_events'][-MAX_RECENT_EVENTS:]


# ══════════════════════════════════════════════════════════════════════
# SIGNATURE VERIFICATION
# ══════════════════════════════════════════════════════════════════════
def verify_retell_signature(payload_body, signature):
    """Verify RetellAI webhook signature using HMAC-SHA256."""
    if not RETELL_API_KEY:
        log.warning("RETELL_API_KEY not set — skipping signature verification")
        return True

    if not signature:
        return False

    expected = hmac.new(
        RETELL_API_KEY.encode('utf-8'),
        payload_body,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(expected, signature)


# ══════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════

@app.route('/webhook/retell/tool', methods=['POST'])
def webhook_tool():
    """Handle custom tool calls from RetellAI Violet agent.

    RetellAI POSTs when the LLM triggers a custom tool during conversation.
    The response is returned to the agent as the tool result.
    """
    # 1. Parse payload (no HMAC verification for tool calls — custom tool
    #    calls from RetellAI don't send webhook-style signatures; they use
    #    whatever headers we configured on the tool definition)
    raw_body = request.get_data()

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        log.warning("Invalid JSON in tool call body")
        return '', 400

    global _last_tool_payload
    _last_tool_payload = payload

    tool_name = payload.get('name', '')
    # RetellAI sends "call" for phone calls, "chat" for SMS chats
    call_data = payload.get('call', payload.get('chat', {}))
    args = payload.get('args', {})

    # Log raw payload structure for debugging
    log.info(f"Tool payload keys: {list(payload.keys())}")
    log.info(f"call_data keys: {list(call_data.keys()) if call_data else 'EMPTY'}")
    if call_data:
        dv = call_data.get('retell_llm_dynamic_variables', call_data.get('dynamic_variables', {}))
        log.info(f"dynamic_variables keys: {list(dv.keys()) if dv else 'EMPTY'}")
    # Log full payload for first few calls to diagnose structure
    log.info(f"FULL PAYLOAD: {json.dumps(payload)[:2000]}")

    # Merge call_data — RetellAI sends retell_llm_dynamic_variables inside the call/chat object
    chat = {
        **call_data,
        'chat_id': call_data.get('call_id', call_data.get('chat_id', 'unknown')),
    }
    # Ensure retell_llm_dynamic_variables is present (already in call_data from spread)
    if 'retell_llm_dynamic_variables' not in chat:
        chat['retell_llm_dynamic_variables'] = {}
    chat_id = chat['chat_id']

    log.info(f"[{chat_id[:12]}] Tool call: {tool_name}")

    # 3. Route to handler
    try:
        if tool_name == 'notify_first_response':
            result = violet_core.handle_first_response(chat, args)
            _record_event('first_response', chat_id, args.get('response_summary', '')[:80], source='tool')

        elif tool_name == 'notify_candidate_optout':
            result = violet_core.handle_optout(chat, args, notify_fn=send_notification)
            _record_event('optout', chat_id, args.get('optout_text', '')[:80], source='tool')

        elif tool_name == 'notify_conversation_complete':
            result = violet_core.handle_conversation_complete(chat, args, notify_fn=send_notification)
            action = result.get('status', 'unknown')
            if action == 'lead_created':
                _record_event('created', chat_id, f"interest={args.get('interest_level', '')}", source='tool')
            elif action == 'lead_exists':
                _record_event('duplicate', chat_id, 'Form Submission already exists', source='tool')
            else:
                _record_event(action, chat_id, result.get('message', ''), source='tool')

        elif tool_name == 'notify_candidate_qualified':
            result = violet_core.handle_qualified(chat, args, notify_fn=send_notification)
            action = result.get('status', 'unknown')
            if action in ('qualified_lead_created',):
                _record_event('created', chat_id, f"QUALIFIED: {args.get('qualification_summary', '')[:60]}", source='tool')
            else:
                _record_event(action, chat_id, result.get('message', ''), source='tool')

        else:
            log.warning(f"[{chat_id[:12]}] Unknown tool: {tool_name}")
            result = {'status': 'unknown_tool', 'message': f'Unknown tool: {tool_name}'}
            _record_event('skip', chat_id, f'unknown tool: {tool_name}', source='tool')

    except Exception as e:
        log.exception(f"[{chat_id[:12]}] Unhandled error in tool handler: {tool_name}")
        _record_event('error', chat_id, str(e), source='tool')
        result = {'status': 'error', 'message': 'Internal error processing tool call'}

    # Return response to agent (agent sees this in conversation)
    return jsonify(result), 200


@app.route('/webhook/retell', methods=['POST'])
def webhook_retell():
    """Receive and process RetellAI chat_analyzed webhooks (fallback).

    This fires after RetellAI's auto-close timeout (~6 hours). It serves as
    a safety net: enriches Form Submissions created by tool handlers, or
    creates new ones for conversations where tools didn't fire.
    """
    # 1. Verify signature
    raw_body = request.get_data()
    signature = request.headers.get('x-retell-signature', '')

    if not verify_retell_signature(raw_body, signature):
        log.warning("Invalid webhook signature — rejected")
        return '', 401

    # 2. Parse payload
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        log.warning("Invalid JSON in webhook body")
        return '', 400

    # 3. Only process chat_analyzed events
    event = payload.get('event', '')
    if event != 'chat_analyzed':
        log.info(f"Ignoring event type: {event}")
        return '', 204

    chat = payload.get('data', payload.get('chat', payload))
    chat_id = chat.get('chat_id', 'unknown')

    log.info(f"[{chat_id[:12]}] Received chat_analyzed webhook (fallback)")

    # 4. Process through fallback handler
    try:
        result = violet_core.handle_chat_analyzed(chat, notify_fn=send_notification)

        action = result.get('action', 'unknown')
        detail = result.get('detail', '')

        _record_event(action, chat_id, detail, source='webhook')

        # If SF create/update failed, save to dead letter
        if action == 'error':
            dead_letter.append(chat, result, detail)

    except Exception as e:
        log.exception(f"[{chat_id[:12]}] Unhandled error processing webhook")
        _record_event('error', chat_id, str(e), source='webhook')
        dead_letter.append(chat, {'chat_id': chat_id}, str(e))

    # Always return 204 — never make RetellAI retry
    return '', 204


@app.route('/health', methods=['GET'])
def health():
    """Health check — verifies SF connectivity and returns uptime."""
    sf_ok = False
    sf_detail = ''
    try:
        token, url = get_salesforce_credentials()
        sf_ok = bool(token and url)
        sf_detail = url if sf_ok else 'no credentials'
    except Exception as e:
        sf_detail = str(e)[:200]

    start = datetime.fromisoformat(_stats['start_time'])
    uptime_seconds = (datetime.now(timezone.utc) - start).total_seconds()

    return jsonify({
        'status': 'healthy' if sf_ok else 'degraded',
        'salesforce': {
            'connected': sf_ok,
            'instance': sf_detail if sf_ok else None,
            'error': sf_detail if not sf_ok else None,
        },
        'uptime_seconds': int(uptime_seconds),
        'dead_letter_count': dead_letter.count(),
        'stats': {
            'webhooks_received': _stats['webhooks_received'],
            'tool_calls_received': _stats['tool_calls_received'],
            'created': _stats['created'],
            'enriched': _stats['enriched'],
            'optouts': _stats['optouts'],
            'apply_now_received': _stats['apply_now_received'],
            'apply_now_sent': _stats['apply_now_sent'],
            'errors': _stats['errors'],
        },
    })


@app.route('/status', methods=['GET'])
def status():
    """HTML dashboard showing service stats."""
    start = datetime.fromisoformat(_stats['start_time'])
    uptime_seconds = (datetime.now(timezone.utc) - start).total_seconds()

    hours = int(uptime_seconds // 3600)
    minutes = int((uptime_seconds % 3600) // 60)
    uptime_str = f"{hours}h {minutes}m"

    sf_ok = False
    sf_instance = ''
    try:
        token, url = get_salesforce_credentials()
        sf_ok = bool(token and url)
        sf_instance = url
    except Exception:
        pass

    dl_count = dead_letter.count()

    return render_template('status.html',
        uptime=uptime_str,
        sf_connected=sf_ok,
        sf_instance=sf_instance,
        webhooks_received=_stats['webhooks_received'],
        tool_calls_received=_stats['tool_calls_received'],
        created=_stats['created'],
        duplicates=_stats['duplicates'],
        enriched=_stats['enriched'],
        skipped=_stats['skipped'],
        optouts=_stats['optouts'],
        first_responses=_stats['first_responses'],
        errors=_stats['errors'],
        dead_letter_count=dl_count,
        last_webhook=_stats['last_webhook'] or 'never',
        last_tool_call=_stats['last_tool_call'] or 'never',
        last_created=_stats['last_created'] or 'never',
        recent_events=list(reversed(_stats['recent_events'][-20:])),
    )


@app.route('/api/debug/last-tool-payload', methods=['GET'])
def debug_last_payload():
    """Return the last raw tool call payload for debugging."""
    return jsonify(_last_tool_payload)


@app.route('/api/retry-failed', methods=['POST'])
def retry_failed():
    """Replay all entries in the dead letter queue."""
    entries = dead_letter.read_all()
    if not entries:
        return jsonify({'message': 'Dead letter queue is empty', 'retried': 0})

    results = []
    for entry in entries:
        chat = entry.get('chat_payload', {})
        chat_id = entry.get('chat_id', 'unknown')

        try:
            result = violet_core.handle_chat_analyzed(
                chat,
                notify_fn=send_notification,
            )
            results.append({
                'chat_id': chat_id,
                'action': result.get('action'),
                'detail': result.get('detail'),
            })
            _record_event(result.get('action', 'retry'), chat_id, result.get('detail', ''))
        except Exception as e:
            results.append({
                'chat_id': chat_id,
                'action': 'error',
                'detail': str(e)[:200],
            })

    # Clear the dead letter queue
    archive_path, cleared = dead_letter.clear()

    created = sum(1 for r in results if r['action'] == 'created')
    failed = sum(1 for r in results if r['action'] == 'error')

    return jsonify({
        'retried': len(results),
        'created': created,
        'failed': failed,
        'archived': archive_path,
        'results': results,
    })


# ══════════════════════════════════════════════════════════════════════
# APPLY NOW — After-hours instant response to Apply Now submissions
# ══════════════════════════════════════════════════════════════════════

@app.route('/webhook/sf/apply-now', methods=['POST'])
def webhook_apply_now():
    """Handle Apply Now form submission notifications from Salesforce trigger.

    Phase 1: Logs raw payload for discovery (always runs).
    Phase 2: Parses fields, validates, checks after-hours + opt-out,
             sends RetellAI SMS, logs to Blackthorn.

    Requires APPLY_NOW_AGENT_ID and APPLY_NOW_FROM_NUMBER env vars
    to be set for Phase 2 SMS sending. Without them, logs only.
    """
    raw_body = request.get_data()

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        log.warning("Invalid JSON in apply-now webhook body")
        return '', 400

    # Always log raw payload for debugging / payload discovery
    log.info(f"APPLY_NOW PAYLOAD: {json.dumps(payload)[:3000]}")
    _record_event('apply_now_received', 'sf-trigger',
                  json.dumps(payload)[:120], source='webhook')

    # Process through Apply Now handler
    result = {'status': 'received'}
    try:
        result = violet_core.handle_apply_now(payload)
        status = result.get('status', 'received')
        contact_id = result.get('contact_id', 'unknown')

        log.info(f"APPLY_NOW RESULT: status={status}, "
                 f"contact={contact_id}, msg={result.get('message', '')}")

        if status == 'sent':
            _record_event('apply_now_sent', contact_id[:12],
                          result.get('message', '')[:120], source='webhook')

    except Exception as e:
        log.exception(f"Error processing apply-now webhook: {e}")
        _record_event('error', 'sf-trigger', f'apply_now: {str(e)[:100]}',
                      source='webhook')
        result = {'status': 'error', 'message': str(e)[:200]}

    # Always return 200 — never make SF trigger retry
    return jsonify(result), 200


# ══════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log.info(f"Starting Violet Webhook Service v2 on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
