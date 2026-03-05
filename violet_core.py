"""Violet Core — Business logic for real-time Violet → Salesforce lead handoff.

Handles custom tool triggers from RetellAI Violet agent and chat_analyzed
fallback webhooks. Creates Form Submissions + Tasks instead of Job Applicants.

Handlers:
  handle_first_response()        — Log engagement when candidate replies
  handle_optout()                — Update SF Contact opt-out fields
  handle_conversation_complete() — Create/update Form Submission + Task for interested leads
  handle_qualified()             — Create/update Form Submission (high priority) + Task
  handle_chat_analyzed()         — Fallback: enrich or create from post-chat analysis
  handle_apply_now()             — After-hours instant SMS response to Apply Now submissions
"""

import logging
import os
import threading
import time
from datetime import date, datetime, timezone

import requests
from salesforce_client import sf_query_all, get_salesforce_credentials

log = logging.getLogger('violet_core')

# Agents to skip (no longer active or no job data)
SKIP_AGENTS = {
    'SMS Violet - EMR Trainer Outreach',
    'Violet - MedPro Inbound Lead Agent',
}

# Form Submission constants
FORM_SOURCE = 'Violet AI'
FORM_TYPE = 'Apply Now - Bot'
RECORD_TYPE_ID = '0123m0000019N8uAAE'

# Interest levels that warrant lead creation
INTERESTED_LEVELS = ('very_interested', 'somewhat_interested')

# Recruiter assignment pools — round-robin within each pool
# Bypasses Natterbox Distribution Engine (no DE access for Violet leads)
RECRUITER_POOLS = {
    'nursing': [
        '005A0000004xrDUIAY',  # Camesha Pitterson
        '0053m00000CV1gnAAD',  # Jean-Carlos Beltran
        '005cx000001AzODAA0',  # Bryant Salter
        '005cx000008TXs1AAG',  # Ronnie Diaz
        '0052G00000AdNNYQA3',  # Sabrina Cornell
    ],
    'allied': [
        '0053m00000Dm03gAAB',  # Joshua Mayer
        '0052G000005Q7UQQA0',  # Aleanna Vargas
        '005cx0000007ySTAAY',  # Stephen Williams
    ],
}

# Thread-safe round-robin counters (reset on restart — acceptable at this volume)
_rr_lock = threading.Lock()
_rr_counters = {'nursing': 0, 'allied': 0}

# Apply Now dedup tracking (in-memory, reset on restart — acceptable at this volume)
_apply_now_lock = threading.Lock()
_apply_now_sent = set()  # Set of form_submission_ids already processed


# ══════════════════════════════════════════════════════════════════════
# ID EXTRACTION (unchanged from v1)
# ══════════════════════════════════════════════════════════════════════

def extract_contact_id(chat):
    """Extract Salesforce Contact ID from chat data."""
    dv = chat.get('retell_llm_dynamic_variables') or {}
    meta = chat.get('metadata') or {}

    cid = dv.get('candidate_id', meta.get('candidate_id', ''))
    if cid and cid.startswith('003') and len(cid) >= 15:
        return cid

    url = dv.get('candidate_salesforce_url', '')
    if url and '/Contact/' in url:
        extracted = url.split('/Contact/')[1].split('/')[0]
        if extracted.startswith('003') and len(extracted) >= 15:
            return extracted

    return ''


def extract_job_id(chat):
    """Extract Salesforce Job ID from chat data."""
    dv = chat.get('retell_llm_dynamic_variables') or {}

    url = dv.get('job_salesforce_url', '')
    if url and '/AVTRRT__Job__c/' in url:
        return url.split('/AVTRRT__Job__c/')[1].split('/')[0]

    j18 = dv.get('job_ID_18', '')
    if j18 and j18.startswith('a0F'):
        return j18

    return ''


# ══════════════════════════════════════════════════════════════════════
# SALESFORCE HELPERS
# ══════════════════════════════════════════════════════════════════════

def check_existing_submissions(contact_id, job_id=None):
    """Check if a Form Submission already exists for this contact+job.

    Returns:
        Form Submission ID if exists, None otherwise
    """
    if not contact_id:
        return None

    if job_id:
        soql = f"SELECT Id FROM Form_Submission__c WHERE Contact_Candidate__c = '{contact_id}' AND Job__c = '{job_id}' AND Source__c = '{FORM_SOURCE}' LIMIT 1"
    else:
        soql = f"SELECT Id FROM Form_Submission__c WHERE Contact_Candidate__c = '{contact_id}' AND Source__c = '{FORM_SOURCE}' ORDER BY CreatedDate DESC LIMIT 1"

    try:
        records = sf_query_all(soql)
        if records:
            return records[0].get('Id')
    except Exception as e:
        log.warning(f"Dedup query failed: {e}")

    return None


def check_existing_task(contact_id, job_id=None):
    """Check if a Violet AI Task already exists for this contact+job today.

    Returns:
        Task ID if exists, None otherwise
    """
    if not contact_id:
        return None

    soql = f"SELECT Id FROM Task WHERE WhoId = '{contact_id}' AND Activity_Type__c = 'Violet AI SMS Screening' AND CreatedDate = TODAY"
    if job_id:
        soql += f" AND WhatId = '{job_id}'"
    soql += " LIMIT 1"

    try:
        records = sf_query_all(soql)
        if records:
            return records[0].get('Id')
    except Exception as e:
        log.warning(f"Task dedup query failed: {e}")

    return None


def create_form_submission(record):
    """Create a Form_Submission__c record in Salesforce.

    Args:
        record: dict with contact_id, job_id, lead_outcome, summary,
                is_qualified, and optional candidate/job detail fields

    Returns:
        (success: bool, result: dict)
    """
    sf_record = {
        'attributes': {'type': 'Form_Submission__c'},
        'Contact_Candidate__c': record['contact_id'],
        'Source__c': FORM_SOURCE,
        'Form_Type__c': FORM_TYPE,
        'RecordTypeId': RECORD_TYPE_ID,
        'Lead_Outcome__c': record.get('lead_outcome', 'Interested - Violet AI'),
        'Short_Code_Text_Opt_In__c': False,
        'Seeing__c': False,
        'Seeking_Sponsorship_to_Work_in_US__c': False,
        'No_Jobs_Available_Specialty_QA__c': False,
        'Willing_to_Work_in_Arkansas__c': False,
    }

    # Job reference
    if record.get('job_id'):
        sf_record['Job__c'] = record['job_id']

    # Qualified flags
    if record.get('is_qualified'):
        sf_record['Hot_Job_Application__c'] = True
        sf_record['Priority_Submit_Candidate__c'] = True

    # Candidate details from dynamic variables
    # Note: Your_Specialty__c omitted — restricted picklist uses full names
    # (e.g., "Registered Nurse") but dynamic_variables have abbreviations ("RN").
    # Specialty is already on the Contact record.
    for field, key in [
        ('Job_Title__c', 'job_title'),
        ('Job_City__c', 'job_city'),
        ('Job_State__c', 'job_state'),
        ('Your_First__c', 'candidate_first_name'),
        ('Your_Last__c', 'candidate_last_name'),
        ('Your_Phone__c', 'candidate_phone'),
        ('Your_Email__c', 'candidate_email'),
    ]:
        val = record.get(key, '')
        if val:
            sf_record[field] = val

    # Your_Email__c is REQUIRED — Apex trigger uses it as the primary key
    # for contact matching. Generate a fallback if not provided.
    if 'Your_Email__c' not in sf_record:
        contact_id = record.get('contact_id', 'unknown')
        sf_record['Your_Email__c'] = f'{contact_id}@violet-ai.medpro.com'

    # Conversation summary
    if record.get('summary'):
        sf_record['Questions_Comments__c'] = record['summary'][:3000]

    return _sf_composite_create([sf_record])


def update_form_submission(submission_id, fields):
    """Update an existing Form_Submission__c record.

    Args:
        submission_id: SF record ID
        fields: dict of fields to update

    Returns:
        (success: bool, result: dict)
    """
    sf_record = {
        'attributes': {'type': 'Form_Submission__c'},
        'Id': submission_id,
    }
    sf_record.update(fields)

    return _sf_composite_update([sf_record])


def _build_task_description(transcript, args):
    """Build Description field with highlights + full transcript (32K limit)."""
    parts = []

    # Highlights from qualification data
    highlights = []
    if args.get('qualification_summary'):
        highlights.append(f"Qualification: {args['qualification_summary']}")
    if args.get('interest_level'):
        highlights.append(f"Interest: {args['interest_level']}")
    if args.get('available_start'):
        highlights.append(f"Available: {args['available_start']}")
    if args.get('certifications'):
        highlights.append(f"Certifications: {args['certifications']}")
    if args.get('license_type'):
        highlights.append(f"License: {args['license_type']}")
    if args.get('experience_months'):
        highlights.append(f"Experience: {args['experience_months']} months")
    if args.get('preferred_contact'):
        highlights.append(f"Preferred contact: {args['preferred_contact']}")
    if args.get('conversation_summary'):
        highlights.append(f"Summary: {args['conversation_summary']}")

    if highlights:
        parts.append("=== KEY HIGHLIGHTS ===")
        parts.extend(highlights)
        parts.append("")

    # Full transcript
    if transcript:
        parts.append("=== FULL TRANSCRIPT ===")
        if isinstance(transcript, str):
            parts.append(transcript)
        elif isinstance(transcript, list):
            for msg in transcript:
                role = msg.get('role', '?')
                content = msg.get('content', '')
                if content:
                    parts.append(f"{role.capitalize()}: {content}")

    return '\n'.join(parts)[:30000] if parts else ''


def _build_task_comments(args):
    """Build Comments field — brief summary only (255 char limit)."""
    parts = []
    if args.get('qualification_summary'):
        parts.append(args['qualification_summary'])
    elif args.get('conversation_summary'):
        parts.append(args['conversation_summary'])
    return '; '.join(parts)[:255] if parts else ''


def _build_call_result(args, tier):
    """Build Call/Text Result field from tool args."""
    parts = [f"{'Qualified' if tier == 'qualified' else 'Interested'}"]
    if args.get('interest_level'):
        parts.append(f"interest={args['interest_level']}")
    if args.get('available_start'):
        parts.append(f"available {args['available_start']}")
    if args.get('certifications'):
        parts.append(f"certs: {args['certifications']}")
    return ' | '.join(parts)


def create_contact_task(record):
    """Create a Task record linked to a Contact and optionally a Job.

    Args:
        record: dict with contact_id, job_id, subject, description,
                priority ('High' or 'Normal'), transcript, args, department

    Returns:
        (success: bool, result: dict)
    """
    # Determine Category based on department
    dept = record.get('department', '').lower()
    if 'allied' in dept:
        category = 'Candidate - Allied Bot'
    else:
        category = 'Candidate - Nursing Bot'

    # Build description with highlights + transcript (32K limit)
    args = record.get('args', {})
    transcript = record.get('transcript', '')
    description = _build_task_description(transcript, args)

    # Build brief comments (255 char limit)
    comments = _build_task_comments(args)

    # Build call result summary
    tier = record.get('tier', 'interested')
    call_result = _build_call_result(args, tier)

    sf_record = {
        'attributes': {'type': 'Task'},
        'WhoId': record['contact_id'],
        'Subject': record.get('subject', 'Violet AI Lead'),
        'Description': description or record.get('description', '')[:30000],
        'Status': 'Open',
        'Priority': record.get('priority', 'Normal'),
        'IsReminderSet': True,
        'ReminderDateTime': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000+0000'),
        'ActivityDate': date.today().isoformat(),
        'Type': 'AI Screening',
        'ContactCandidate_Type__c': 'Domestic',
        'Category__c': category,
        'Activity_Type__c': 'Violet AI SMS Screening',
        'AVTRRT__Call_Result__c': call_result[:255],
        'AVTRRT__Comments__c': comments,
    }

    if record.get('job_id'):
        sf_record['WhatId'] = record['job_id']

    if record.get('owner_id'):
        sf_record['OwnerId'] = record['owner_id']

    return _sf_composite_create([sf_record])


def update_contact_lead_status(contact_id, recruiter_id=''):
    """Set Contact Lead_Status__c to 'Hot lead - current' to trigger recruiter notification.

    IMPORTANT: recruiter_id must be included in the same PATCH as Lead_Status__c.
    The notification flow (Hot_Lead_Notification_Domestic) requires
    AVTRRT__Recruiter__c IsChanged=True in the same transaction as
    Lead_Status__c='Hot lead - current' — separate API calls won't trigger it.

    Returns:
        (success: bool, result: dict)
    """
    sf_record = {
        'attributes': {'type': 'Contact'},
        'Id': contact_id,
        'Lead_Status__c': 'Hot lead - current',
        'Source__c': 'Violet AI',
    }

    if recruiter_id:
        sf_record['AVTRRT__Recruiter__c'] = recruiter_id

    return _sf_composite_update([sf_record])


def select_recruiter(department):
    """Pick next recruiter from the round-robin pool (no SF write).

    Returns:
        recruiter_id: str (empty string if no pool configured)
    """
    pool_key = 'allied' if 'allied' in (department or '').lower() else 'nursing'
    pool = RECRUITER_POOLS.get(pool_key, [])

    if not pool:
        log.warning(f"No recruiters configured for pool: {pool_key}")
        return ''

    with _rr_lock:
        idx = _rr_counters[pool_key] % len(pool)
        recruiter_id = pool[idx]
        _rr_counters[pool_key] += 1

    log.info(f"Selected recruiter {recruiter_id} ({pool_key}[{idx}])")
    return recruiter_id


def assign_recruiter(contact_id, department):
    """Assign a recruiter to a Contact via round-robin.

    Determines Nursing vs Allied pool from department, picks next recruiter,
    and PATCHes AVTRRT__Recruiter__c on the Contact.

    Args:
        contact_id: SF Contact ID (003...)
        department: job_medpro_dept value from dynamic variables

    Returns:
        (success: bool, recruiter_id: str)
    """
    recruiter_id = select_recruiter(department)
    if not recruiter_id:
        return False, ''

    # PATCH Contact
    sf_record = {
        'attributes': {'type': 'Contact'},
        'Id': contact_id,
        'AVTRRT__Recruiter__c': recruiter_id,
    }

    success, result = _sf_composite_update([sf_record])
    if success:
        log.info(f"Assigned recruiter {recruiter_id} to Contact {contact_id}")
    else:
        log.error(f"Recruiter assignment failed for Contact {contact_id}: {result}")

    return success, recruiter_id


def process_optout(contact_id):
    """Update SF Contact with opt-out fields.

    Returns:
        (success: bool, result: dict)
    """
    sf_record = {
        'attributes': {'type': 'Contact'},
        'Id': contact_id,
        'simplesms__DoNotSMS__c': True,
        'Text_Opt_In__c': False,
        'Text_Opt_Out_Date__c': date.today().isoformat(),
        'Long_Code_Text_Opt_Out__c': date.today().isoformat(),
    }

    return _sf_composite_update([sf_record])


def _sf_composite_create(records):
    """POST to SF Composite API to create records.

    Returns:
        (success: bool, result: dict)
    """
    access_token, instance_url = get_salesforce_credentials()

    payload = {'allOrNone': False, 'records': records}
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                f'{instance_url}/services/data/v59.0/composite/sobjects',
                headers=headers,
                json=payload,
                timeout=120,
            )
            break
        except requests.exceptions.ReadTimeout:
            log.warning(f"SF timeout, attempt {attempt + 1}/3")
            if attempt < 2:
                access_token, instance_url = get_salesforce_credentials()
                headers['Authorization'] = f'Bearer {access_token}'
                time.sleep(2)
            else:
                return False, {'error': 'timeout after 3 attempts'}

    if resp.status_code == 200:
        api_results = resp.json()
        result = api_results[0]
        if result.get('success'):
            obj_type = records[0]['attributes']['type']
            log.info(f"CREATED {obj_type}: {result['id']}")
            return True, {'id': result['id']}
        else:
            err = str(result.get('errors', []))
            log.error(f"SF create failed: {err}")
            return False, {'error': err}
    else:
        log.error(f"SF API error {resp.status_code}: {resp.text[:300]}")
        return False, {'error': f'HTTP {resp.status_code}: {resp.text[:300]}'}


def _sf_composite_update(records):
    """PATCH to SF Composite API to update records.

    Returns:
        (success: bool, result: dict)
    """
    access_token, instance_url = get_salesforce_credentials()

    payload = {'allOrNone': False, 'records': records}
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    for attempt in range(3):
        try:
            resp = requests.patch(
                f'{instance_url}/services/data/v59.0/composite/sobjects',
                headers=headers,
                json=payload,
                timeout=120,
            )
            break
        except requests.exceptions.ReadTimeout:
            log.warning(f"SF timeout, attempt {attempt + 1}/3")
            if attempt < 2:
                access_token, instance_url = get_salesforce_credentials()
                headers['Authorization'] = f'Bearer {access_token}'
                time.sleep(2)
            else:
                return False, {'error': 'timeout after 3 attempts'}

    if resp.status_code == 200:
        api_results = resp.json()
        result = api_results[0]
        if result.get('success'):
            obj_type = records[0]['attributes']['type']
            log.info(f"UPDATED {obj_type}: {result.get('id', records[0].get('Id', ''))}")
            return True, {'id': result.get('id', records[0].get('Id', ''))}
        else:
            err = str(result.get('errors', []))
            log.error(f"SF update failed: {err}")
            return False, {'error': err}
    else:
        log.error(f"SF API error {resp.status_code}: {resp.text[:300]}")
        return False, {'error': f'HTTP {resp.status_code}: {resp.text[:300]}'}


# ══════════════════════════════════════════════════════════════════════
# BLACKTHORN SMS — Log Violet AI messages to simplesms__SMS_Message__c
# ══════════════════════════════════════════════════════════════════════

def create_blackthorn_sms(contact_id, from_phone, to_phone, message, chat_id=None,
                          sms_type='Incoming', sms_status='Received'):
    """Log a Violet AI conversation to Blackthorn SMS.

    chat_id links this response record to the outbound campaign SMS
    via simplesms__Sid__c (same chat_id on both records).

    For outbound Apply Now responses, use sms_type='Outgoing', sms_status='Sent'.
    """
    sf_record = {
        'attributes': {'type': 'simplesms__SMS_Message__c'},
        'simplesms__Contact__c': contact_id,
        'simplesms__From_Phone__c': from_phone,
        'simplesms__To_Phone__c': to_phone,
        'simplesms__From_Num__c': from_phone,
        'simplesms__To__c': to_phone,
        'simplesms__Message__c': message[:255],
        'simplesms__Message_Full__c': message,
        'simplesms__Message_Date__c': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000+0000'),
        'simplesms__Status__c': sms_status,
        'simplesms__Type__c': sms_type,
        'simplesms__Bulk_Message__c': False,
    }
    if chat_id:
        sf_record['simplesms__Sid__c'] = chat_id
    return _sf_composite_create([sf_record])


# ══════════════════════════════════════════════════════════════════════
# CONVERSATION ANALYSIS (server-side, replaces waiting for RetellAI)
# ══════════════════════════════════════════════════════════════════════

def analyze_conversation(transcript, args):
    """Analyze conversation transcript and tool args to build lead details.

    Combines the LLM-provided tool args with transcript parsing to
    produce a structured summary for the Form Submission.

    Args:
        transcript: list of message dicts from RetellAI
        args: dict of tool parameters filled by the LLM

    Returns:
        dict with: summary, lead_outcome, is_qualified
    """
    interest = args.get('interest_level', 'undecided')

    # Build summary from args + transcript
    parts = []
    if args.get('conversation_summary'):
        parts.append(args['conversation_summary'])
    if args.get('qualification_summary'):
        parts.append(f"Qualifications: {args['qualification_summary']}")
    if args.get('license_type'):
        parts.append(f"License: {args['license_type']}")
    if args.get('certifications'):
        parts.append(f"Certifications: {args['certifications']}")
    if args.get('experience_months'):
        parts.append(f"Experience: {args['experience_months']} months")
    if args.get('available_start'):
        parts.append(f"Available: {args['available_start']}")
    if args.get('preferred_contact'):
        parts.append(f"Preferred contact: {args['preferred_contact']}")

    candidate_available = args.get('candidate_available', False)
    has_credentials = args.get('has_required_credentials', False)

    if candidate_available:
        parts.append("Candidate is available")
    if has_credentials:
        parts.append("Has required credentials")

    # Add transcript snippet if no summary from args
    if not parts and transcript:
        messages = []
        for msg in transcript[-10:]:
            role = msg.get('role', '')
            content = msg.get('content', '')
            if content:
                messages.append(f"{role}: {content}")
        if messages:
            parts.append("Recent messages:\n" + '\n'.join(messages))

    summary = '\n'.join(parts) if parts else 'No details available'

    # Determine lead outcome
    is_qualified = bool(
        interest == 'very_interested'
        and candidate_available
        and has_credentials
    )

    if is_qualified or args.get('qualification_summary'):
        lead_outcome = 'Qualified - Violet AI'
    elif interest in INTERESTED_LEVELS:
        lead_outcome = 'Interested - Violet AI'
    else:
        lead_outcome = 'Contacted - Violet AI'

    return {
        'summary': summary,
        'lead_outcome': lead_outcome,
        'is_qualified': is_qualified,
    }


# ══════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════════════

def _extract_dynamic_vars(chat):
    """Pull candidate/job details from dynamic variables into a flat dict."""
    dv = chat.get('retell_llm_dynamic_variables') or {}
    return {
        'candidate_first_name': dv.get('candidate_first_name', ''),
        'candidate_last_name': dv.get('candidate_last_name', ''),
        'candidate_phone': dv.get('candidate_phone', ''),
        'candidate_email': dv.get('candidate_email', ''),
        'candidate_specialty': dv.get('candidate_specialty', ''),
        'job_title': dv.get('job_title', ''),
        'job_city': dv.get('job_city', ''),
        'job_state': dv.get('job_state', ''),
        'job_medpro_dept': dv.get('job_medpro_dept', ''),
    }


def _build_task_subject(tier, dv):
    """Build a descriptive Task subject line."""
    tier_label = 'QUALIFIED' if tier == 'qualified' else 'INTERESTED'
    job_desc = dv.get('job_title', 'Unknown')
    location = f"{dv.get('job_city', '')}"
    if dv.get('job_state'):
        location += f" {dv['job_state']}"
    location = location.strip()

    if location:
        return f"Violet AI: {tier_label} - {job_desc}, {location}"
    return f"Violet AI: {tier_label} - {job_desc}"


# ══════════════════════════════════════════════════════════════════════
# APPLY NOW — After-hours instant response to Apply Now submissions
# ══════════════════════════════════════════════════════════════════════

def is_after_hours():
    """Check if current time is outside M-F 8:30am-5:30pm ET business hours."""
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo('America/New_York'))
    except (ImportError, KeyError):
        # Fallback for environments without tzdata (e.g., Windows)
        now = datetime.now(timezone(timedelta(hours=-5)))
    # Weekend
    if now.weekday() >= 5:
        return True
    # Before 8:30am
    if now.hour < 8 or (now.hour == 8 and now.minute < 30):
        return True
    # After 5:30pm
    if now.hour > 17 or (now.hour == 17 and now.minute >= 30):
        return True
    return False


def normalize_phone(phone):
    """Normalize a phone number to E.164 format (+1XXXXXXXXXX).

    Returns E.164 string or empty string if invalid.
    """
    if not phone:
        return ''
    digits = ''.join(c for c in str(phone) if c.isdigit())
    if len(digits) == 10:
        return f'+1{digits}'
    if len(digits) == 11 and digits[0] == '1':
        return f'+{digits}'
    if phone.startswith('+') and len(digits) >= 10:
        return f'+{digits}'
    return ''


def check_contact_optout(contact_id):
    """Check if a Contact has opted out of SMS.

    Returns True if opted out, False otherwise.
    """
    soql = f"SELECT simplesms__DoNotSMS__c, Long_Code_Text_Opt_Out__c FROM Contact WHERE Id = '{contact_id}' LIMIT 1"
    try:
        records = sf_query_all(soql)
        if records:
            rec = records[0]
            if rec.get('simplesms__DoNotSMS__c'):
                return True
            if rec.get('Long_Code_Text_Opt_Out__c'):
                return True
    except Exception as e:
        log.warning(f"Opt-out check failed for {contact_id}: {e}")
    return False


def check_apply_now_sent(form_submission_id):
    """Check if we already sent an Apply Now SMS for this Form Submission."""
    with _apply_now_lock:
        return form_submission_id in _apply_now_sent


def record_apply_now_sent(form_submission_id, chat_id):
    """Record that we sent an Apply Now SMS for this Form Submission."""
    with _apply_now_lock:
        _apply_now_sent.add(form_submission_id)
    log.info(f"APPLY_NOW: recorded sent for FS {form_submission_id}, chat={chat_id}")


def send_apply_now_sms(contact_id, phone_e164, candidate_vars, job_vars,
                       form_submission_id, from_number):
    """Send Violet SMS to an Apply Now applicant via RetellAI API.

    Creates an SMS chat using the Apply Now agent. Returns chat_id on success,
    None on failure.
    """
    retell_key = os.environ.get('RETELL_API_KEY', '')
    agent_id = os.environ.get('APPLY_NOW_AGENT_ID', '')

    if not retell_key or not agent_id:
        log.error("APPLY_NOW: RETELL_API_KEY or APPLY_NOW_AGENT_ID not configured")
        return None

    # Build dynamic variables — includes all existing campaign vars plus Apply Now markers
    dynamic_variables = {
        **candidate_vars,
        **job_vars,
        'form_submission_id': form_submission_id,
        'response_source': 'apply_now',
    }

    payload = {
        'from_number': from_number,
        'to_number': phone_e164,
        'override_agent_id': agent_id,
        'retell_llm_dynamic_variables': dynamic_variables,
    }

    try:
        resp = requests.post(
            'https://api.retellai.com/create-sms-chat',
            headers={
                'Authorization': f'Bearer {retell_key}',
                'Content-Type': 'application/json',
            },
            json=payload,
            timeout=30,
        )
    except Exception as e:
        log.error(f"APPLY_NOW: RetellAI API error: {e}")
        return None

    if resp.status_code in (200, 201):
        data = resp.json()
        chat_id = data.get('chat_id', '')
        log.info(f"APPLY_NOW: SMS sent — chat_id={chat_id}, contact={contact_id}")
        return chat_id
    else:
        log.error(f"APPLY_NOW: RetellAI API {resp.status_code}: {resp.text[:300]}")
        return None


def handle_apply_now(payload):
    """Handle incoming Apply Now form submission from SF trigger.

    Validates the payload, checks after-hours + opt-out, then sends
    RetellAI SMS via send_apply_now_sms().

    Field names are best-guess from SF trigger — adapt after Phase 1 discovery.

    Returns:
        dict with status, message, and optional contact_id/chat_id
    """
    # Extract fields from payload — try multiple naming conventions
    # until we see the real payload format from SF trigger
    contact_id = (payload.get('contact_id') or payload.get('ContactId')
                  or payload.get('Contact_Candidate__c') or '')
    form_submission_id = (payload.get('form_submission_id') or payload.get('Id')
                         or payload.get('FormSubmissionId') or '')
    phone = (payload.get('phone') or payload.get('Phone')
             or payload.get('Your_Phone__c') or '')
    first_name = (payload.get('first_name') or payload.get('FirstName')
                  or payload.get('Your_First__c') or '')
    last_name = (payload.get('last_name') or payload.get('LastName')
                 or payload.get('Your_Last__c') or '')
    email = (payload.get('email') or payload.get('Email')
             or payload.get('Your_Email__c') or '')
    job_id = (payload.get('job_id') or payload.get('JobId')
              or payload.get('Job__c') or '')
    job_title = (payload.get('job_title') or payload.get('JobTitle')
                 or payload.get('Job_Title__c') or '')
    job_city = (payload.get('job_city') or payload.get('JobCity')
                or payload.get('Job_City__c') or '')
    job_state = (payload.get('job_state') or payload.get('JobState')
                 or payload.get('Job_State__c') or '')
    job_dept = (payload.get('job_dept') or payload.get('Department')
                or payload.get('job_medpro_dept') or '')

    log.info(f"APPLY_NOW: contact={contact_id}, phone={phone}, "
             f"job={job_title}, fs={form_submission_id}")

    # Validate required fields
    if not contact_id:
        return {'status': 'skipped', 'message': 'Missing contact_id'}
    if not phone:
        # Try to look up phone from SF Contact
        try:
            records = sf_query_all(
                f"SELECT MobilePhone, Phone FROM Contact WHERE Id = '{contact_id}' LIMIT 1"
            )
            if records:
                phone = records[0].get('MobilePhone') or records[0].get('Phone') or ''
        except Exception as e:
            log.warning(f"APPLY_NOW: Phone lookup failed: {e}")

        if not phone:
            return {'status': 'skipped', 'message': 'Missing phone number'}

    # Check agent configuration
    agent_id = os.environ.get('APPLY_NOW_AGENT_ID', '')
    from_number = os.environ.get('APPLY_NOW_FROM_NUMBER', '')
    if not agent_id or not from_number:
        log.info("APPLY_NOW: Agent not configured (Phase 1 — logging only)")
        return {'status': 'logged', 'message': 'Payload logged, agent not configured yet',
                'contact_id': contact_id}

    # After-hours check — during business hours, recruiters handle directly
    # _bypass_hours_check flag allows testing during business hours
    bypass_hours = payload.get('_bypass_hours_check', False)
    if not bypass_hours and not is_after_hours():
        log.info(f"APPLY_NOW: Business hours — recruiters handle, skipping SMS")
        return {'status': 'skipped_business_hours',
                'message': 'Business hours — recruiters handle',
                'contact_id': contact_id}

    # Opt-out check
    if check_contact_optout(contact_id):
        log.info(f"APPLY_NOW: Contact {contact_id} opted out — skipping SMS")
        return {'status': 'skipped_optout', 'message': 'Contact opted out of SMS',
                'contact_id': contact_id}

    # Dedup: don't re-send if already sent for this Form Submission
    if form_submission_id and check_apply_now_sent(form_submission_id):
        log.info(f"APPLY_NOW: Already sent for FS {form_submission_id}")
        return {'status': 'skipped_dedup',
                'message': 'Already sent for this form submission',
                'contact_id': contact_id}

    # Normalize phone to E.164
    phone_e164 = normalize_phone(phone)
    if not phone_e164:
        return {'status': 'skipped', 'message': f'Invalid phone number: {phone}',
                'contact_id': contact_id}

    # Query SF for missing job details if we have job_id but no title
    if job_id and not job_title:
        try:
            records = sf_query_all(
                f"SELECT Name, AVTRRT__City__c, AVTRRT__State__c, MedPro_Dept__c FROM AVTRRT__Job__c WHERE Id = '{job_id}' LIMIT 1"
            )
            if records:
                job_title = records[0].get('Name', '')
                job_city = job_city or records[0].get('AVTRRT__City__c', '')
                job_state = job_state or records[0].get('AVTRRT__State__c', '')
                job_dept = job_dept or records[0].get('MedPro_Dept__c', '')
        except Exception as e:
            log.warning(f"APPLY_NOW: Job lookup failed: {e}")

    # Build dynamic variables for RetellAI agent
    candidate_vars = {
        'candidate_id': contact_id,
        'candidate_first_name': first_name,
        'candidate_last_name': last_name,
        'candidate_email': email,
        'candidate_phone': phone_e164,
    }
    job_vars = {
        'job_ID_18': job_id,
        'job_title': job_title,
        'job_city': job_city,
        'job_state': job_state,
        'job_medpro_dept': job_dept,
    }

    # Send SMS via RetellAI
    chat_id = send_apply_now_sms(
        contact_id=contact_id,
        phone_e164=phone_e164,
        candidate_vars=candidate_vars,
        job_vars=job_vars,
        form_submission_id=form_submission_id,
        from_number=from_number,
    )

    if not chat_id:
        return {'status': 'error', 'message': 'RetellAI SMS send failed',
                'contact_id': contact_id}

    # Track that we sent for this FS (dedup)
    if form_submission_id:
        record_apply_now_sent(form_submission_id, chat_id)

    # Log outbound SMS to Blackthorn for recruiter visibility
    bt_msg = f"Violet AI - Apply Now Response: {job_title}"
    try:
        create_blackthorn_sms(
            contact_id=contact_id,
            from_phone=from_number,
            to_phone=phone_e164,
            message=bt_msg,
            chat_id=chat_id,
            sms_type='Outgoing',
            sms_status='Sent',
        )
    except Exception as e:
        log.warning(f"APPLY_NOW: Blackthorn SMS log failed: {e}")

    log.info(f"APPLY_NOW: Complete — SMS sent to {contact_id}, chat_id={chat_id}")
    return {
        'status': 'sent',
        'message': f'SMS sent, chat_id={chat_id}',
        'contact_id': contact_id,
        'chat_id': chat_id,
    }


# ══════════════════════════════════════════════════════════════════════
# TOOL HANDLERS — Called from app.py webhook routes
# ══════════════════════════════════════════════════════════════════════

def handle_first_response(chat, args):
    """Handle notify_first_response tool call.

    Logs that a candidate replied. No SF write.

    Returns:
        dict with status and message (returned to agent)
    """
    contact_id = extract_contact_id(chat)
    chat_id = chat.get('chat_id', chat.get('call_id', 'unknown'))
    summary = args.get('response_summary', '')

    log.info(f"[{chat_id[:12]}] FIRST_RESPONSE: contact={contact_id}, summary={summary[:100]}")

    return {
        'status': 'acknowledged',
        'message': 'Response logged',
    }


def handle_optout(chat, args, notify_fn=None):
    """Handle notify_candidate_optout tool call.

    Updates SF Contact with opt-out fields immediately.

    Returns:
        dict with status and message (returned to agent)
    """
    contact_id = extract_contact_id(chat)
    chat_id = chat.get('chat_id', chat.get('call_id', 'unknown'))
    optout_text = args.get('optout_text', '')

    log.info(f"[{chat_id[:12]}] OPT_OUT: contact={contact_id}, text={optout_text[:50]}")

    if not contact_id:
        log.warning(f"[{chat_id[:12]}] OPT_OUT: no contact ID — cannot update SF")
        return {
            'status': 'opt_out_recorded',
            'message': 'Candidate has been opted out',
        }

    success, result = process_optout(contact_id)

    if success:
        log.info(f"[{chat_id[:12]}] OPT_OUT: SF Contact updated")
        # Log opt-out to Blackthorn SMS for recruiter visibility
        dv = chat.get('retell_llm_dynamic_variables') or {}
        candidate_phone = dv.get('candidate_phone', '')
        summary_msg = "[Violet AI - Opt Out] " + optout_text[:500]
        try:
            create_blackthorn_sms(contact_id, candidate_phone, '', summary_msg, chat_id=chat_id)
        except Exception as e:
            log.warning(f"[{chat_id[:12]}] Blackthorn SMS log failed: {e}")
        if notify_fn:
            notify_fn('optout', {
                'chat_id': chat_id,
                'contact_id': contact_id,
                'optout_text': optout_text,
            })
    else:
        log.error(f"[{chat_id[:12]}] OPT_OUT: SF update failed: {result}")

    return {
        'status': 'opt_out_recorded',
        'message': 'Candidate has been opted out',
    }


def handle_conversation_complete(chat, args, notify_fn=None):
    """Handle notify_conversation_complete tool call.

    Creates Form Submission + Task for interested candidates.

    Returns:
        dict with status and message (returned to agent)
    """
    contact_id = extract_contact_id(chat)
    job_id = extract_job_id(chat)
    chat_id = chat.get('chat_id', chat.get('call_id', 'unknown'))
    dv_fields = _extract_dynamic_vars(chat)

    interest = args.get('interest_level', 'undecided')
    log.info(f"[{chat_id[:12]}] CONVERSATION_COMPLETE: contact={contact_id}, interest={interest}")

    # Only create leads for interested candidates
    if interest not in INTERESTED_LEVELS:
        log.info(f"[{chat_id[:12]}] SKIP: not interested ({interest})")
        return {
            'status': 'noted',
            'message': 'Conversation outcome recorded',
        }

    if not contact_id:
        log.warning(f"[{chat_id[:12]}] SKIP: no contact ID")
        return {'status': 'error', 'message': 'Missing contact ID'}

    if not job_id:
        log.warning(f"[{chat_id[:12]}] SKIP: no job ID")
        return {'status': 'error', 'message': 'Missing job ID'}

    # Apply Now detection — form_submission_id in dynamic vars means this is
    # a response to an Apply Now application (FS already exists in SF)
    dv = chat.get('retell_llm_dynamic_variables') or {}
    is_apply_now = dv.get('response_source') == 'apply_now'
    apply_now_fs_id = dv.get('form_submission_id', '') if is_apply_now else ''

    # Dedup check (outbound campaigns only — Apply Now uses explicit form_submission_id)
    if not is_apply_now:
        existing_id = check_existing_submissions(contact_id, job_id)
        if existing_id:
            log.info(f"[{chat_id[:12]}] DEDUP: Form Submission {existing_id} already exists")
            return {
                'status': 'lead_exists',
                'message': 'Lead already recorded for recruiter review',
            }

    # Analyze conversation
    transcript = chat.get('transcript', [])
    analysis = analyze_conversation(transcript, args)

    if apply_now_fs_id:
        # Apply Now: update existing Form Submission from the application
        log.info(f"[{chat_id[:12]}] APPLY_NOW: updating Form Submission {apply_now_fs_id}")
        update_fields = {
            'Lead_Outcome__c': analysis['lead_outcome'],
        }
        if analysis['summary']:
            update_fields['Questions_Comments__c'] = analysis['summary'][:3000]
        upd_ok, upd_result = update_form_submission(apply_now_fs_id, update_fields)
        if not upd_ok:
            log.error(f"[{chat_id[:12]}] ERROR: Form Submission update failed: {upd_result}")
        submission_id = apply_now_fs_id
    else:
        # Create new Form Submission (outbound campaign)
        record = {
            'contact_id': contact_id,
            'job_id': job_id,
            'lead_outcome': analysis['lead_outcome'],
            'is_qualified': False,
            'summary': analysis['summary'],
            **dv_fields,
        }
        fs_ok, fs_result = create_form_submission(record)
        if not fs_ok:
            err_detail = fs_result.get('error', 'unknown')
            log.error(f"[{chat_id[:12]}] ERROR: Form Submission create failed: {fs_result}")
            return {'status': 'error', 'message': f'Failed to create lead record: {err_detail}'}

        submission_id = fs_result.get('id', '')

    # Select recruiter from pool (round-robin only, no SF write yet)
    dept = dv_fields.get('job_medpro_dept', '')
    rr_id = select_recruiter(dept)

    # Create Task
    task_record = {
        'contact_id': contact_id,
        'job_id': job_id,
        'subject': _build_task_subject('interested', dv_fields),
        'description': analysis['summary'],
        'priority': 'Normal',
        'transcript': transcript,
        'args': args,
        'department': dv_fields.get('job_medpro_dept', ''),
        'tier': 'interested',
        'owner_id': rr_id,
    }
    task_ok, task_result = create_contact_task(task_record)
    if not task_ok:
        log.warning(f"[{chat_id[:12]}] Task create failed (Form Submission still created): {task_result}")

    # Combined: Lead_Status + Recruiter in same PATCH to trigger notification Flow
    # (Hot_Lead_Notification_Domestic requires AVTRRT__Recruiter__c IsChanged=True
    #  in the same transaction as Lead_Status__c='Hot lead - current')
    ls_ok, ls_result = update_contact_lead_status(contact_id, recruiter_id=rr_id)
    if not ls_ok:
        log.warning(f"[{chat_id[:12]}] Lead_Status__c + Recruiter update failed: {ls_result}")

    log.info(f"[{chat_id[:12]}] CREATED: Form Submission {submission_id}")

    # Log conversation to Blackthorn SMS for recruiter visibility
    candidate_phone = dv_fields.get('candidate_phone', '')
    summary_msg = "[Violet AI - Interested] " + analysis['summary'][:500]
    try:
        create_blackthorn_sms(contact_id, candidate_phone, '', summary_msg, chat_id=chat_id)
    except Exception as e:
        log.warning(f"[{chat_id[:12]}] Blackthorn SMS log failed: {e}")

    if notify_fn:
        notify_fn('created', {
            'chat_id': chat_id,
            'contact_id': contact_id,
            'job_id': job_id,
            'submission_id': submission_id,
            'task_id': task_result.get('id', '') if task_ok else '',
            'tier': 'interested',
            'lead_outcome': analysis['lead_outcome'],
            'job_desc': f"{dv_fields.get('job_title', '')} in {dv_fields.get('job_city', '')}, {dv_fields.get('job_state', '')}",
            'summary': analysis['summary'][:200],
            'agent': chat.get('agent_name', ''),
        })

    return {
        'status': 'lead_created',
        'message': 'Lead recorded for recruiter review',
    }


def handle_qualified(chat, args, notify_fn=None):
    """Handle notify_candidate_qualified tool call.

    Creates or updates Form Submission with high priority + Task.

    Returns:
        dict with status and message (returned to agent)
    """
    contact_id = extract_contact_id(chat)
    job_id = extract_job_id(chat)
    chat_id = chat.get('chat_id', chat.get('call_id', 'unknown'))
    dv_fields = _extract_dynamic_vars(chat)

    log.info(f"[{chat_id[:12]}] QUALIFIED: contact={contact_id}, quals={args.get('qualification_summary', '')[:80]}")

    if not contact_id:
        log.warning(f"[{chat_id[:12]}] SKIP: no contact ID")
        return {'status': 'error', 'message': 'Missing contact ID'}

    if not job_id:
        log.warning(f"[{chat_id[:12]}] SKIP: no job ID")
        return {'status': 'error', 'message': 'Missing job ID'}

    # Analyze with qualification data
    transcript = chat.get('transcript', [])
    analysis = analyze_conversation(transcript, {
        **args,
        'interest_level': 'very_interested',
        'candidate_available': True,
        'has_required_credentials': True,
    })

    # Check if Form Submission already exists (from conversation_complete or Apply Now)
    dv = chat.get('retell_llm_dynamic_variables') or {}
    apply_now_fs_id = dv.get('form_submission_id', '') if dv.get('response_source') == 'apply_now' else ''
    existing_id = apply_now_fs_id or check_existing_submissions(contact_id, job_id)

    if existing_id:
        # Update existing to qualified
        update_fields = {
            'Lead_Outcome__c': 'Qualified - Violet AI',
            'Hot_Job_Application__c': True,
            'Priority_Submit_Candidate__c': True,
        }
        if analysis['summary']:
            update_fields['Questions_Comments__c'] = analysis['summary'][:3000]

        upd_ok, upd_result = update_form_submission(existing_id, update_fields)
        submission_id = existing_id

        if not upd_ok:
            log.error(f"[{chat_id[:12]}] ERROR: Form Submission update failed: {upd_result}")
            return {'status': 'error', 'message': 'Failed to update lead record'}

        log.info(f"[{chat_id[:12]}] UPDATED to QUALIFIED: Form Submission {existing_id}")
    else:
        # Create new qualified Form Submission
        record = {
            'contact_id': contact_id,
            'job_id': job_id,
            'lead_outcome': 'Qualified - Violet AI',
            'is_qualified': True,
            'summary': analysis['summary'],
            **dv_fields,
        }
        fs_ok, fs_result = create_form_submission(record)
        if not fs_ok:
            err_detail = fs_result.get('error', 'unknown')
            log.error(f"[{chat_id[:12]}] ERROR: Form Submission create failed: {fs_result}")
            return {'status': 'error', 'message': f'Failed to create lead record: {err_detail}'}

        submission_id = fs_result.get('id', '')
        log.info(f"[{chat_id[:12]}] CREATED QUALIFIED: Form Submission {submission_id}")

    # Only assign recruiter + create Task if conversation_complete didn't already
    existing_task_id = check_existing_task(contact_id, job_id)
    rr_id = ''  # default — set below if we need to assign

    if existing_id and existing_task_id:
        # conversation_complete already created FS + Task + assigned recruiter
        log.info(f"[{chat_id[:12]}] SKIP recruiter/task: already handled by conversation_complete (Task {existing_task_id})")
        task_ok = True
        task_result = {'id': existing_task_id}
    else:
        # First time seeing this lead — select recruiter + create Task
        dept = dv_fields.get('job_medpro_dept', '')
        rr_id = select_recruiter(dept)

        task_record = {
            'contact_id': contact_id,
            'job_id': job_id,
            'subject': _build_task_subject('qualified', dv_fields),
            'description': analysis['summary'],
            'priority': 'High',
            'transcript': transcript,
            'args': args,
            'department': dv_fields.get('job_medpro_dept', ''),
            'tier': 'qualified',
            'owner_id': rr_id,
        }
        task_ok, task_result = create_contact_task(task_record)
        if not task_ok:
            log.warning(f"[{chat_id[:12]}] Task create failed (Form Submission still created): {task_result}")

    # Combined: Lead_Status + Recruiter in same PATCH to trigger notification Flow
    # For existing leads (conversation_complete already handled), recruiter_id='' means
    # only Lead_Status is updated. For new leads, both fields change in one transaction.
    ls_ok, ls_result = update_contact_lead_status(contact_id, recruiter_id=rr_id)
    if not ls_ok:
        log.warning(f"[{chat_id[:12]}] Lead_Status__c + Recruiter update failed: {ls_result}")

    # Log conversation to Blackthorn SMS for recruiter visibility
    candidate_phone = dv_fields.get('candidate_phone', '')
    summary_msg = "[Violet AI - Qualified] " + analysis['summary'][:500]
    try:
        create_blackthorn_sms(contact_id, candidate_phone, '', summary_msg, chat_id=chat_id)
    except Exception as e:
        log.warning(f"[{chat_id[:12]}] Blackthorn SMS log failed: {e}")

    if notify_fn:
        notify_fn('created', {
            'chat_id': chat_id,
            'contact_id': contact_id,
            'job_id': job_id,
            'submission_id': submission_id,
            'task_id': task_result.get('id', '') if task_ok else '',
            'tier': 'qualified',
            'lead_outcome': 'Qualified - Violet AI',
            'job_desc': f"{dv_fields.get('job_title', '')} in {dv_fields.get('job_city', '')}, {dv_fields.get('job_state', '')}",
            'summary': analysis['summary'][:200],
            'agent': chat.get('agent_name', ''),
        })

    return {
        'status': 'qualified_lead_created',
        'message': 'Qualified lead recorded, recruiter will be notified',
        'task_created': task_ok,
        'task_error': str(task_result) if not task_ok else None,
    }


def handle_chat_analyzed(chat, notify_fn=None):
    """Handle chat_analyzed webhook (fallback from RetellAI auto-close).

    If a Form Submission already exists → enrich with AI analysis data.
    If no Form Submission exists → create one from chat_analysis data.
    Safety net for conversations where custom tools weren't triggered.

    Returns:
        dict with action, detail, contact_id, job_id, chat_id
    """
    chat_id = chat.get('chat_id', 'unknown')
    result = {'chat_id': chat_id, 'action': None, 'detail': None}

    # Check if agent should be skipped
    agent = chat.get('agent_name', '')
    if agent in SKIP_AGENTS:
        result['action'] = 'skip'
        result['detail'] = f'agent skipped: {agent}'
        log.info(f"[{chat_id[:12]}] SKIP: {result['detail']}")
        return result

    # Skip ongoing chats
    status = chat.get('chat_status', '')
    if status == 'ongoing':
        result['action'] = 'skip'
        result['detail'] = 'chat still ongoing'
        log.info(f"[{chat_id[:12]}] SKIP: ongoing")
        return result

    # Extract IDs
    contact_id = extract_contact_id(chat)
    job_id = extract_job_id(chat)
    result['contact_id'] = contact_id
    result['job_id'] = job_id

    if not contact_id:
        result['action'] = 'skip'
        result['detail'] = 'no contact ID in chat data'
        log.warning(f"[{chat_id[:12]}] SKIP: no contact ID")
        return result

    if not job_id:
        result['action'] = 'skip'
        result['detail'] = 'no job ID in chat data'
        log.warning(f"[{chat_id[:12]}] SKIP: no job ID")
        return result

    # Parse analysis data
    ca = chat.get('chat_analysis') or {}
    custom = ca.get('custom_analysis_data') or {}

    if not custom:
        result['action'] = 'skip'
        result['detail'] = 'no analysis data'
        log.info(f"[{chat_id[:12]}] SKIP: no analysis data")
        return result

    # Handle opt-out in analysis
    if custom.get('opted_out'):
        result['action'] = 'skip'
        result['detail'] = 'opted out'
        log.info(f"[{chat_id[:12]}] SKIP: opted out (handled by tool or during analysis)")
        return result

    # Determine interest/qualification from analysis
    qual = custom.get('qualification_result', '')
    interest = custom.get('interest_level', '')
    is_qualified = qual in ('fully_qualified', 'partially_qualified')
    is_interested = interest in INTERESTED_LEVELS

    if not is_qualified and not is_interested:
        result['action'] = 'skip'
        result['detail'] = f'not qualified/interested (qual={qual}, interest={interest})'
        log.info(f"[{chat_id[:12]}] SKIP: {result['detail']}")
        return result

    dv_fields = _extract_dynamic_vars(chat)
    summary = (custom.get('conversation_summary') or ca.get('chat_summary', ''))[:3000]

    # Check if Form Submission already exists (created by tool handlers)
    existing_id = check_existing_submissions(contact_id, job_id)

    if existing_id:
        # Enrich existing Form Submission with analysis data
        update_fields = {}
        if is_qualified:
            update_fields['Lead_Outcome__c'] = 'Qualified - Violet AI'
            update_fields['Hot_Job_Application__c'] = True
            update_fields['Priority_Submit_Candidate__c'] = True
        if summary:
            update_fields['Questions_Comments__c'] = summary

        if update_fields:
            upd_ok, upd_result = update_form_submission(existing_id, update_fields)
            if upd_ok:
                result['action'] = 'enriched'
                result['detail'] = f'Form Submission {existing_id} enriched with analysis'
                log.info(f"[{chat_id[:12]}] ENRICHED: {existing_id}")
            else:
                result['action'] = 'error'
                result['detail'] = f'Failed to enrich: {upd_result}'
                log.error(f"[{chat_id[:12]}] ERROR enriching: {upd_result}")
        else:
            result['action'] = 'duplicate'
            result['detail'] = 'Form Submission already exists, no enrichment needed'
            log.info(f"[{chat_id[:12]}] DEDUP: {existing_id} already exists")
        return result

    # No existing Form Submission — create from analysis (tools didn't fire)
    lead_outcome = 'Qualified - Violet AI' if is_qualified else 'Interested - Violet AI'

    record = {
        'contact_id': contact_id,
        'job_id': job_id,
        'lead_outcome': lead_outcome,
        'is_qualified': is_qualified,
        'summary': summary,
        **dv_fields,
    }

    fs_ok, fs_result = create_form_submission(record)

    if not fs_ok:
        result['action'] = 'error'
        result['detail'] = fs_result.get('error', 'unknown SF error')
        log.error(f"[{chat_id[:12]}] ERROR: {result['detail']}")
        if notify_fn:
            notify_fn('error', {
                'chat_id': chat_id,
                'contact_id': contact_id,
                'job_id': job_id,
                'error': result['detail'],
            })
        return result

    submission_id = fs_result.get('id', '')

    # Check if Task already exists (from tool handlers)
    existing_task_id = check_existing_task(contact_id, job_id)

    if existing_task_id:
        log.info(f"[{chat_id[:12]}] SKIP recruiter/task: already exists (Task {existing_task_id})")
        task_ok = True
        task_result = {'id': existing_task_id}
    else:
        # Assign recruiter from pool (before Task so OwnerId is set correctly)
        dept = dv_fields.get('job_medpro_dept', '')
        rr_ok, rr_id = assign_recruiter(contact_id, dept)
        if not rr_ok:
            log.warning(f"[{chat_id[:12]}] Recruiter assignment failed")

        # Create Task
        tier = 'qualified' if is_qualified else 'interested'
        fallback_args = {
            'interest_level': interest,
            'qualification_summary': custom.get('qualification_tier', ''),
            'conversation_summary': summary,
        }
        task_record = {
            'contact_id': contact_id,
            'job_id': job_id,
            'subject': _build_task_subject(tier, dv_fields),
            'description': summary,
            'priority': 'High' if is_qualified else 'Normal',
            'transcript': chat.get('transcript', ''),
            'args': fallback_args,
            'department': dv_fields.get('job_medpro_dept', ''),
            'tier': tier,
            'owner_id': rr_id if rr_ok else '',
        }
        task_ok, task_result = create_contact_task(task_record)

    # Update Contact Lead_Status__c to trigger recruiter notification Flows
    ls_ok, ls_result = update_contact_lead_status(contact_id)
    if not ls_ok:
        log.warning(f"[{chat_id[:12]}] Lead_Status__c update failed: {ls_result}")

    result['action'] = 'created'
    result['detail'] = f'Form Submission {submission_id} created (fallback)'
    log.info(f"[{chat_id[:12]}] CREATED (fallback): {submission_id}")

    if notify_fn:
        notify_fn('created', {
            'chat_id': chat_id,
            'contact_id': contact_id,
            'job_id': job_id,
            'submission_id': submission_id,
            'task_id': task_result.get('id', '') if task_ok else '',
            'tier': tier,
            'lead_outcome': lead_outcome,
            'job_desc': f"{dv_fields.get('job_title', '')} in {dv_fields.get('job_city', '')}, {dv_fields.get('job_state', '')}",
            'summary': summary[:200],
            'agent': agent,
        })

    return result
