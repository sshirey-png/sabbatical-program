"""
Sabbatical Program - Flask Backend
FirstLine Schools
"""

import os
import json
import uuid
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, request, jsonify, send_file, session, redirect, url_for
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
from google.cloud import bigquery
from authlib.integrations.flask_client import OAuth

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
# Trust proxy headers (required for Cloud Run to detect https)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
CORS(app)

# Configuration
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT', 'talent-demo-482004')
DATASET_ID = 'sabbatical'
TABLE_ID = 'applications'

# Email Configuration
SMTP_EMAIL = os.environ.get('SMTP_EMAIL', 'talent@firstlineschools.org')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
TALENT_TEAM_EMAIL = 'talent@firstlineschools.org'
SABBATICAL_ADMIN_EMAIL = 'sshirey@firstlineschools.org'  # Additional admin for sabbatical notifications
HR_EMAIL = 'hr@firstlineschools.org'
BENEFITS_EMAIL = 'benefits@firstlineschools.org'
PAYROLL_EMAIL = 'payroll@firstlineschools.org'
CEO_EMAIL = 'spence@firstlineschools.org'

# Network-level admins - can see ALL sabbatical applications
# This includes C-Team, HR leadership, and ExDir of Teaching and Learning
SABBATICAL_NETWORK_ADMINS = [
    # C-Team
    'sshirey@firstlineschools.org',      # Chief People Officer
    'brichardson@firstlineschools.org',  # Chief of Human Resources
    'spence@firstlineschools.org',       # CEO
    'sdomango@firstlineschools.org',     # Chief Experience Officer
    'dcavato@firstlineschools.org',      # C-Team
    # HR/Talent Team
    'talent@firstlineschools.org',
    'hr@firstlineschools.org',
    'awatts@firstlineschools.org',
    'jlombas@firstlineschools.org',
    'tcole@firstlineschools.org',
    # Academic Leadership
    'kfeil@firstlineschools.org',        # ExDir of Teaching and Learning
]

# Job titles that grant school-level admin access (can see their school's applications)
SABBATICAL_SCHOOL_LEADER_TITLES = [
    'school director',
    'principal',
    'assistant principal',
    'head of school',
]

# Legacy ADMIN_USERS for backward compatibility
ADMIN_USERS = SABBATICAL_NETWORK_ADMINS

# Email aliases - map alternative emails to primary FirstLine emails
# Format: 'alternate@email.com': 'primary@firstlineschools.org'
EMAIL_ALIASES = {
    'zach@esynola.org': 'zodonnell@firstlineschools.org',
}


def resolve_email_alias(email):
    """
    Resolve an email alias to the primary FirstLine email.
    Returns the primary email if an alias exists, otherwise returns the original email.
    """
    if not email:
        return email
    return EMAIL_ALIASES.get(email.lower(), email)


def get_sabbatical_admin_access(email):
    """
    Determine the user's admin access level for the sabbatical program.

    Returns a dict with:
    - {'level': 'network'} - Can see all applications across the network
    - {'level': 'school', 'school': 'Location Name'} - Can see their school's applications
    - {'level': 'none'} - No admin access
    """
    if not email:
        return {'level': 'none'}

    email_lower = email.lower()

    # 1. Check if network-level admin (C-Team, HR, kfeil)
    if email_lower in [e.lower() for e in SABBATICAL_NETWORK_ADMINS]:
        return {'level': 'network'}

    # 2. Check job title for C-Team or school leader access
    try:
        query = """
        SELECT Job_Title, Location_Name
        FROM `talent-demo-482004.talent_grow_observations.staff_master_list_with_function`
        WHERE LOWER(Email_Address) = @email
        AND Employment_Status IN ('Active', 'Leave of absence')
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email_lower)]
        )
        results = list(bq_client.query(query, job_config=job_config).result())

        if results:
            job_title = (results[0].Job_Title or '').lower()
            location = results[0].Location_Name or ''

            # Check if C-Team (Chief or Executive Director)
            if 'chief' in job_title or 'ex dir' in job_title or 'executive dir' in job_title:
                return {'level': 'network'}

            # Check if job title matches school leader patterns
            for leader_title in SABBATICAL_SCHOOL_LEADER_TITLES:
                if leader_title in job_title:
                    return {'level': 'school', 'school': location}
    except Exception as e:
        logger.error(f"Error checking sabbatical admin access: {e}")

    return {'level': 'none'}


# Status values and their display order
STATUS_VALUES = [
    'Submitted',
    'Tentatively Approved',
    'Plan Submitted',
    'Approved',
    'Completed',
    'Denied',
    'Withdrawn'
]

# Sabbatical options with pay percentages
SABBATICAL_OPTIONS = {
    '8 Weeks - 100% Salary': {'weeks': 8, 'salary_pct': 100},
    '10 Weeks - 80% Salary': {'weeks': 10, 'salary_pct': 80},
    '12 Weeks - 67% Salary': {'weeks': 12, 'salary_pct': 67}
}

# BigQuery client
bq_client = bigquery.Client(project=PROJECT_ID)

# OAuth setup
oauth = OAuth(app)
if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET:
    google = oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'}
    )
else:
    google = None


# ============ Email Functions ============

def send_email(to_email, subject, html_body, cc_emails=None):
    """Send an email using Gmail SMTP."""
    if not SMTP_PASSWORD:
        logger.warning("SMTP_PASSWORD not configured, skipping email")
        return False

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"FirstLine Schools Talent <{SMTP_EMAIL}>"
        msg['To'] = to_email
        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)

        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            recipients = [to_email] + (cc_emails or [])
            server.sendmail(SMTP_EMAIL, recipients, msg.as_string())

        logger.info(f"Email sent to {to_email}: {subject}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def send_application_confirmation(application):
    """Send confirmation email to applicant when they submit."""
    subject = f"Sabbatical Application Received - {application['application_id']}"

    option_info = SABBATICAL_OPTIONS.get(application['sabbatical_option'], {})
    weeks = option_info.get('weeks', 'N/A')
    salary_pct = option_info.get('salary_pct', 'N/A')

    html_body = f"""
    <div style="font-family: 'Open Sans', Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #002f60; padding: 20px; text-align: center;">
            <h1 style="color: white; margin: 0;">Sabbatical Program</h1>
        </div>
        <div style="padding: 30px; background-color: #f8f9fa;">
            <h2 style="color: #002f60;">Thank you for your application!</h2>
            <p>Hi {application['employee_name']},</p>
            <p>We've received your sabbatical application. The Talent team will review your request and contact you with next steps.</p>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0;">
                <p style="margin: 5px 0;"><strong>Application ID:</strong> {application['application_id']}</p>
                <p style="margin: 5px 0;"><strong>Option:</strong> {weeks} weeks at {salary_pct}% salary</p>
                <p style="margin: 5px 0;"><strong>Start Date:</strong> {application.get('start_date') or 'TBD'}</p>
                <p style="margin: 5px 0;"><strong>End Date:</strong> {application.get('end_date') or 'TBD'}</p>
            </div>

            <p><strong>What's next?</strong></p>
            <ul>
                <li>Your application will be reviewed by the Talent team</li>
                <li>We may reach out if we need additional information</li>
                <li>You'll receive an email once a decision has been made</li>
            </ul>

            <div style="background-color: #e47727; border-radius: 8px; padding: 15px; margin: 20px 0; text-align: center;">
                <a href="https://sabbatical-program-965913991496.us-central1.run.app/my-sabbatical"
                   style="color: white; text-decoration: none; font-weight: bold;">
                    Check Your Application Status
                </a>
            </div>

            <p style="color: #666; font-size: 0.9em; margin-top: 30px;">Questions? Contact talent@firstlineschools.org</p>
        </div>
        <div style="background-color: #002f60; padding: 15px; text-align: center;">
            <p style="color: white; margin: 0; font-size: 0.9em;">FirstLine Schools - Education For Life</p>
        </div>
    </div>
    """
    # CC the supervisor chain (CEO only for her direct reports)
    supervisor_chain = filter_chain_for_notifications(get_supervisor_chain(application.get('employee_email', '')))
    cc_emails = [s.get('email') for s in supervisor_chain if s.get('email')]

    send_email(application['employee_email'], subject, html_body, cc_emails=cc_emails)


def send_new_application_alert(application):
    """Send alert to Talent team when a new application is submitted."""
    option_info = SABBATICAL_OPTIONS.get(application['sabbatical_option'], {})
    weeks = option_info.get('weeks', 'N/A')
    salary_pct = option_info.get('salary_pct', 'N/A')

    subject = f"New Sabbatical Application: {application['employee_name']}"
    html_body = f"""
    <div style="font-family: 'Open Sans', Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #002f60; padding: 20px; text-align: center;">
            <h1 style="color: white; margin: 0;">New Sabbatical Application</h1>
        </div>
        <div style="padding: 30px; background-color: #f8f9fa;">
            <h2 style="color: #e47727;">New application submitted!</h2>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0;">
                <h3 style="color: #002f60; margin-top: 0;">Employee Information</h3>
                <p style="margin: 5px 0;"><strong>Name:</strong> {application['employee_name']}</p>
                <p style="margin: 5px 0;"><strong>Email:</strong> {application['employee_email']}</p>
            </div>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0;">
                <h3 style="color: #002f60; margin-top: 0;">Sabbatical Details</h3>
                <p style="margin: 5px 0;"><strong>Option:</strong> {weeks} weeks at {salary_pct}% salary</p>
                <p style="margin: 5px 0;"><strong>Start Date:</strong> {application.get('start_date') or 'TBD'}</p>
                <p style="margin: 5px 0;"><strong>End Date:</strong> {application.get('end_date') or 'TBD'}</p>
                <p style="margin: 5px 0;"><strong>Date Flexibility:</strong> {application['date_flexibility']}</p>
                {f"<p style='margin: 5px 0;'><strong>Flexibility Details:</strong> {application['flexibility_explanation']}</p>" if application.get('flexibility_explanation') else ""}
                <p style="margin: 5px 0;"><strong>Manager Discussion:</strong> {application['manager_discussion']}</p>
            </div>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0;">
                <h3 style="color: #002f60; margin-top: 0;">Purpose & Timing</h3>
                <p style="margin: 5px 0;"><strong>Purpose:</strong></p>
                <p style="margin: 5px 0; font-style: italic; color: #666;">"{application['sabbatical_purpose']}"</p>
                <p style="margin: 15px 0 5px 0;"><strong>Why Now:</strong></p>
                <p style="margin: 5px 0; font-style: italic; color: #666;">"{application['why_now']}"</p>
            </div>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0;">
                <h3 style="color: #002f60; margin-top: 0;">Coverage Plan</h3>
                <p style="margin: 5px 0;">{application['coverage_plan']}</p>
            </div>

            <div style="background-color: #fff3cd; border-radius: 8px; padding: 15px; margin: 20px 0;">
                <p style="margin: 0;"><strong>Application ID:</strong> {application['application_id']}</p>
            </div>

            {f"<p><strong>Additional Notes:</strong> {application['additional_notes']}</p>" if application.get('additional_notes') else ""}
        </div>
    </div>
    """
    send_email(TALENT_TEAM_EMAIL, subject, html_body, cc_emails=[SABBATICAL_ADMIN_EMAIL])


def send_status_update(application, old_status, new_status, updated_by, notes=''):
    """Send status update email to applicant."""
    status_messages = {
        'Tentatively Approved': "Great news! Your sabbatical application has been TENTATIVELY APPROVED! Please complete your planning checklist to receive final approval.",
        'Approved': "Congratulations! Your sabbatical application has received FINAL APPROVAL! Your sabbatical dates are now confirmed.",
        'Denied': f"After careful consideration, we are unable to approve your sabbatical request at this time.{' Notes: ' + notes if notes else ''}",
        'Withdrawn': "Your sabbatical application has been withdrawn as requested."
    }

    message = status_messages.get(new_status, f"Your application status has been updated to: {new_status}")

    # Choose color based on status
    if new_status == 'Approved':
        status_color = '#22c55e'  # Green
    elif new_status == 'Tentatively Approved':
        status_color = '#6B46C1'  # Purple
    elif new_status in ['Denied', 'Withdrawn']:
        status_color = '#ef4444'  # Red
    else:
        status_color = '#e47727'  # Orange

    subject = f"Sabbatical Application Update - {new_status}"
    # Add planning page link for tentatively approved applications
    planning_link = ""
    if new_status == 'Tentatively Approved':
        planning_link = f"""
            <div style="background-color: #6B46C1; border-radius: 8px; padding: 20px; margin: 20px 0; text-align: center;">
                <p style="color: white; margin: 0 0 15px 0; font-size: 1.1em;">Start planning your sabbatical now!</p>
                <a href="https://sabbatical-program-965913991496.us-central1.run.app/my-sabbatical"
                   style="display: inline-block; background-color: #D4AF37; color: #002f60; padding: 12px 30px;
                          text-decoration: none; border-radius: 5px; font-weight: bold;">
                    Go to My Sabbatical Planning Page
                </a>
                <p style="color: #ddd; margin: 15px 0 0 0; font-size: 0.9em;">Complete your planning checklist to receive final approval.</p>
            </div>
        """
    elif new_status == 'Approved':
        planning_link = f"""
            <div style="background-color: #22c55e; border-radius: 8px; padding: 20px; margin: 20px 0; text-align: center;">
                <p style="color: white; margin: 0 0 15px 0; font-size: 1.1em;">Your sabbatical is confirmed!</p>
                <a href="https://sabbatical-program-965913991496.us-central1.run.app/my-sabbatical"
                   style="display: inline-block; background-color: white; color: #22c55e; padding: 12px 30px;
                          text-decoration: none; border-radius: 5px; font-weight: bold;">
                    View My Sabbatical Details
                </a>
            </div>
        """

    html_body = f"""
    <div style="font-family: 'Open Sans', Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #002f60; padding: 20px; text-align: center;">
            <h1 style="color: white; margin: 0;">Sabbatical Application Update</h1>
        </div>
        <div style="padding: 30px; background-color: #f8f9fa;">
            <p>Hi {application['employee_name']},</p>

            <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0; text-align: center;">
                <p style="margin: 0 0 10px 0;">Your sabbatical application</p>
                <p style="font-size: 1.5em; color: {status_color}; margin: 0; font-weight: bold;">{new_status}</p>
            </div>

            <p>{message}</p>

            {planning_link}

            <div style="background-color: white; border-radius: 8px; padding: 15px; margin: 20px 0;">
                <p style="margin: 5px 0;"><strong>Application ID:</strong> {application['application_id']}</p>
                <p style="margin: 5px 0;"><strong>Preferred Dates:</strong> {application.get('start_date', '')} - {application.get('end_date', '')}</p>
            </div>

            <p style="color: #666; font-size: 0.9em; margin-top: 30px;">Questions? Contact talent@firstlineschools.org</p>
        </div>
        <div style="background-color: #002f60; padding: 15px; text-align: center;">
            <p style="color: white; margin: 0; font-size: 0.9em;">FirstLine Schools - Education For Life</p>
        </div>
    </div>
    """
    # For Tentatively Approved, CC the supervisor chain (CEO only for her direct reports)
    cc_emails = None
    if new_status == 'Tentatively Approved':
        supervisor_chain = filter_chain_for_notifications(get_supervisor_chain(application.get('employee_email', '')))
        cc_emails = [s.get('email') for s in supervisor_chain if s.get('email')]
        # Also CC Talent admin
        cc_emails.append(SABBATICAL_ADMIN_EMAIL)

    send_email(application['employee_email'], subject, html_body, cc_emails=cc_emails)


# ============ Supervisor Chain Functions ============

def get_supervisor_chain(employee_email):
    """
    Get the supervisor chain for an employee, going up to CEO.
    Returns list of dicts with supervisor name, email, and level.
    """
    try:
        query = """
        WITH RECURSIVE supervisor_chain AS (
            -- Base case: the employee
            SELECT
                Email_Address as email,
                CONCAT(First_Name, ' ', Last_Name) as name,
                Supervisor_Name__Unsecured_ as supervisor_name,
                0 as level
            FROM `talent-demo-482004.talent_grow_observations.staff_master_list_with_function`
            WHERE LOWER(Email_Address) = LOWER(@employee_email)
            AND Employment_Status IN ('Active', 'Leave of absence')

            UNION ALL

            -- Recursive case: find the supervisor's supervisor
            SELECT
                s.Email_Address as email,
                CONCAT(s.First_Name, ' ', s.Last_Name) as name,
                s.Supervisor_Name__Unsecured_ as supervisor_name,
                sc.level + 1 as level
            FROM `talent-demo-482004.talent_grow_observations.staff_master_list_with_function` s
            INNER JOIN supervisor_chain sc
                ON s.Employee_Name__Last_Suffix__First_MI_ = sc.supervisor_name
            WHERE s.Employment_Status IN ('Active', 'Leave of absence')
            AND sc.level < 10  -- Safety limit
        )
        SELECT DISTINCT email, name, supervisor_name, level
        FROM supervisor_chain
        WHERE level > 0  -- Exclude the employee themselves
        ORDER BY level
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("employee_email", "STRING", employee_email)
            ]
        )
        results = bq_client.query(query, job_config=job_config).result()
        chain = []
        for row in results:
            chain.append({
                'email': row.email,
                'name': row.name,
                'level': row.level
            })
        return chain
    except Exception as e:
        logger.error(f"Error getting supervisor chain: {e}")
        return []


def filter_chain_for_notifications(chain):
    """Filter CEO from supervisor chain unless they are a direct report (level 1).
    CEO should only be notified for her direct reports' sabbaticals."""
    return [s for s in chain if s.get('email', '').lower() != CEO_EMAIL.lower() or s.get('level') == 1]


def get_required_approvers(employee_email):
    """
    Get list of all required approvers for final approval.
    Includes: supervisor chain + Talent + HR
    """
    approvers = []

    # Get supervisor chain (CEO only approves for her direct reports)
    chain = filter_chain_for_notifications(get_supervisor_chain(employee_email))
    for supervisor in chain:
        approvers.append({
            'email': supervisor['email'],
            'name': supervisor['name'],
            'role': f"Manager (Level {supervisor['level']})",
            'type': 'manager'
        })

    # Add Talent (sshirey handles Talent approvals)
    approvers.append({
        'email': 'sshirey@firstlineschools.org',
        'name': 'Talent Team',
        'role': 'Talent',
        'type': 'talent'
    })

    # Add HR (brichardson handles HR approvals)
    approvers.append({
        'email': 'brichardson@firstlineschools.org',
        'name': 'HR Team',
        'role': 'HR',
        'type': 'hr'
    })

    return approvers


# ============ BigQuery Functions ============

def get_full_table_id():
    """Get the fully qualified table ID."""
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def ensure_table_exists():
    """Create the BigQuery table if it doesn't exist."""
    try:
        table_id = get_full_table_id()

        # Check if dataset exists, create if not
        dataset_ref = bq_client.dataset(DATASET_ID)
        try:
            bq_client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            bq_client.create_dataset(dataset)
            logger.info(f"Created dataset {DATASET_ID}")

        # Check if table exists
        try:
            bq_client.get_table(table_id)
            return True
        except Exception:
            pass

        # Create table with schema
        schema = [
            bigquery.SchemaField("application_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("submitted_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("employee_name", "STRING"),
            bigquery.SchemaField("employee_email", "STRING"),
            bigquery.SchemaField("employee_location", "STRING"),
            bigquery.SchemaField("sabbatical_option", "STRING"),
            bigquery.SchemaField("preferred_dates", "STRING"),
            bigquery.SchemaField("start_date", "DATE"),
            bigquery.SchemaField("end_date", "DATE"),
            bigquery.SchemaField("date_flexibility", "STRING"),
            bigquery.SchemaField("flexibility_explanation", "STRING"),
            bigquery.SchemaField("sabbatical_purpose", "STRING"),
            bigquery.SchemaField("why_now", "STRING"),
            bigquery.SchemaField("coverage_plan", "STRING"),
            bigquery.SchemaField("manager_discussion", "STRING"),
            bigquery.SchemaField("ack_one_year", "BOOL"),
            bigquery.SchemaField("ack_no_other_job", "BOOL"),
            bigquery.SchemaField("additional_notes", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("status_updated_at", "TIMESTAMP"),
            bigquery.SchemaField("status_updated_by", "STRING"),
            bigquery.SchemaField("admin_notes", "STRING"),
        ]

        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table)
        logger.info(f"Created table {table_id}")
        return True
    except Exception as e:
        logger.error(f"Error ensuring table exists: {e}")
        return False


def row_to_dict(row):
    """Convert a BigQuery row to a dictionary."""
    # Handle both old and new column names
    leave_weeks = getattr(row, 'leave_weeks', None) or 8
    salary_pct = getattr(row, 'salary_percentage', None) or 100
    sabbatical_option = getattr(row, 'sabbatical_option', None) or f"{leave_weeks} Weeks - {salary_pct}% Salary"

    # Get start/end dates (handle both column name formats)
    start_date = getattr(row, 'start_date', None) or getattr(row, 'requested_start_date', None)
    end_date = getattr(row, 'end_date', None) or getattr(row, 'requested_end_date', None)

    # Get flexibility info
    is_flexible = getattr(row, 'flexible', None)
    date_flexibility = getattr(row, 'date_flexibility', None)
    if date_flexibility is None and is_flexible is not None:
        date_flexibility = 'Yes' if is_flexible else 'No'

    flexibility_explanation = getattr(row, 'flexibility_explanation', None) or getattr(row, 'flexibility_details', None) or ''

    # Get manager discussion
    manager_discussed = getattr(row, 'manager_discussed', None)
    manager_discussion = getattr(row, 'manager_discussion', None)
    if manager_discussion is None and manager_discussed is not None:
        manager_discussion = 'Yes' if manager_discussed else 'No'

    # Get location/site
    location = getattr(row, 'employee_location', None) or getattr(row, 'site', None) or ''

    # Get notes
    additional_notes = getattr(row, 'additional_notes', None) or getattr(row, 'additional_comments', None) or ''

    # Get status timestamp
    status_updated_at = getattr(row, 'status_updated_at', None) or getattr(row, 'updated_at', None)

    return {
        'application_id': row.application_id,
        'submitted_at': row.submitted_at.isoformat() if row.submitted_at else '',
        'employee_name': row.employee_name or '',
        'employee_email': row.employee_email or '',
        'employee_location': location,
        'sabbatical_option': sabbatical_option,
        'leave_weeks': leave_weeks,
        'salary_percentage': salary_pct,
        'preferred_dates': getattr(row, 'preferred_dates', None) or '',
        'start_date': start_date.isoformat() if start_date else '',
        'end_date': end_date.isoformat() if end_date else '',
        'date_flexibility': date_flexibility or '',
        'flexibility_explanation': flexibility_explanation,
        'sabbatical_purpose': getattr(row, 'sabbatical_purpose', None) or '',
        'why_now': getattr(row, 'why_now', None) or '',
        'coverage_plan': getattr(row, 'coverage_plan', None) or '',
        'manager_discussion': manager_discussion or '',
        'ack_one_year': getattr(row, 'ack_one_year', False),
        'ack_no_other_job': getattr(row, 'ack_no_other_job', False),
        'additional_notes': additional_notes,
        'status': row.status or '',
        'status_updated_at': status_updated_at.isoformat() if status_updated_at else '',
        'status_updated_by': getattr(row, 'status_updated_by', None) or '',
        'admin_notes': getattr(row, 'admin_notes', None) or ''
    }


def read_all_applications():
    """Read all applications from BigQuery."""
    try:
        ensure_table_exists()
        query = f"""
        SELECT * FROM `{get_full_table_id()}`
        ORDER BY submitted_at DESC
        """
        results = bq_client.query(query).result()
        return [row_to_dict(row) for row in results]
    except Exception as e:
        logger.error(f"Error reading applications: {e}")
        return []


def get_application_by_id(application_id):
    """Get a single application by ID."""
    try:
        query = f"""
        SELECT * FROM `{get_full_table_id()}`
        WHERE application_id = @application_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id)
            ]
        )
        results = bq_client.query(query, job_config=job_config).result()
        for row in results:
            return row_to_dict(row)
        return None
    except Exception as e:
        logger.error(f"Error getting application: {e}")
        return None


def append_application(application_data):
    """Insert a new application into BigQuery."""
    try:
        ensure_table_exists()

        query = f"""
        INSERT INTO `{get_full_table_id()}` (
            application_id, submitted_at, employee_name, employee_email, site,
            leave_weeks, salary_percentage, start_date, end_date,
            flexible, flexibility_details,
            sabbatical_purpose, why_now, coverage_plan, manager_discussed,
            additional_comments, status, created_at, updated_at
        ) VALUES (
            @application_id, @submitted_at, @employee_name, @employee_email, @site,
            @leave_weeks, @salary_percentage, @start_date, @end_date,
            @flexible, @flexibility_details,
            @sabbatical_purpose, @why_now, @coverage_plan, @manager_discussed,
            @additional_comments, @status, @created_at, @updated_at
        )
        """

        submitted_at = datetime.fromisoformat(application_data['submitted_at']) if application_data.get('submitted_at') else datetime.now()
        now = datetime.now()

        # Parse sabbatical_option to get weeks and salary percentage
        sabbatical_option = application_data.get('sabbatical_option', '')
        option_info = SABBATICAL_OPTIONS.get(sabbatical_option, {'weeks': 8, 'salary_pct': 100})
        leave_weeks = option_info['weeks']
        salary_percentage = option_info['salary_pct']

        # Parse start and end dates if provided
        start_date = None
        end_date = None
        if application_data.get('start_date'):
            try:
                start_date = datetime.strptime(application_data['start_date'], '%Y-%m-%d').date()
            except:
                pass
        if application_data.get('end_date'):
            try:
                end_date = datetime.strptime(application_data['end_date'], '%Y-%m-%d').date()
            except:
                pass

        # Convert flexibility to boolean
        flexibility_value = application_data.get('date_flexibility', '')
        flexible = flexibility_value.lower() == 'yes' if isinstance(flexibility_value, str) else bool(flexibility_value)

        # Convert manager discussion to boolean
        manager_value = application_data.get('manager_discussion', '')
        manager_discussed = manager_value.lower() == 'yes' if isinstance(manager_value, str) else bool(manager_value)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_data.get('application_id', '')),
                bigquery.ScalarQueryParameter("submitted_at", "TIMESTAMP", submitted_at),
                bigquery.ScalarQueryParameter("employee_name", "STRING", application_data.get('employee_name', '')),
                bigquery.ScalarQueryParameter("employee_email", "STRING", application_data.get('employee_email', '')),
                bigquery.ScalarQueryParameter("site", "STRING", application_data.get('employee_location', '')),
                bigquery.ScalarQueryParameter("leave_weeks", "INT64", leave_weeks),
                bigquery.ScalarQueryParameter("salary_percentage", "INT64", salary_percentage),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                bigquery.ScalarQueryParameter("flexible", "BOOL", flexible),
                bigquery.ScalarQueryParameter("flexibility_details", "STRING", application_data.get('flexibility_explanation', '')),
                bigquery.ScalarQueryParameter("sabbatical_purpose", "STRING", application_data.get('sabbatical_purpose', '')),
                bigquery.ScalarQueryParameter("why_now", "STRING", application_data.get('why_now', '')),
                bigquery.ScalarQueryParameter("coverage_plan", "STRING", application_data.get('coverage_plan', '')),
                bigquery.ScalarQueryParameter("manager_discussed", "BOOL", manager_discussed),
                bigquery.ScalarQueryParameter("additional_comments", "STRING", application_data.get('additional_notes', '')),
                bigquery.ScalarQueryParameter("status", "STRING", application_data.get('status', 'Submitted')),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", now),
            ]
        )

        bq_client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        logger.error(f"Error appending application: {e}")
        return False


def update_application(application_id, updates):
    """Update an application in BigQuery."""
    try:
        set_clauses = []
        params = [bigquery.ScalarQueryParameter("application_id", "STRING", application_id)]

        # Map old field names to actual table columns
        field_mapping = {
            'status_updated_at': 'updated_at',
            'status_updated_by': None,  # Column doesn't exist, skip
            'admin_notes': None,  # Column doesn't exist, skip
        }

        for field, value in updates.items():
            # Apply field mapping
            actual_field = field_mapping.get(field, field)
            if actual_field is None:
                continue  # Skip fields that don't exist in table

            param_name = f"param_{actual_field}"

            if actual_field == 'updated_at':
                set_clauses.append(f"{actual_field} = @{param_name}")
                if isinstance(value, str):
                    params.append(bigquery.ScalarQueryParameter(param_name, "TIMESTAMP", datetime.fromisoformat(value)))
                else:
                    params.append(bigquery.ScalarQueryParameter(param_name, "TIMESTAMP", value))
            elif actual_field in ['flexible', 'manager_discussed']:
                set_clauses.append(f"{actual_field} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "BOOL", bool(value)))
            else:
                set_clauses.append(f"{actual_field} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "STRING", str(value)))

        if not set_clauses:
            return True

        query = f"""
        UPDATE `{get_full_table_id()}`
        SET {', '.join(set_clauses)}
        WHERE application_id = @application_id
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        bq_client.query(query, job_config=job_config).result()

        return True
    except Exception as e:
        logger.error(f"Error updating application: {e}")
        return False


def require_admin(f):
    """
    Decorator to require admin authentication (network OR school-level).
    Use require_network_admin for operations that need full network access.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = session.get('user')
        if not user:
            return jsonify({'error': 'Authentication required'}), 401

        access = get_sabbatical_admin_access(user.get('email', ''))
        if access['level'] == 'none':
            return jsonify({'error': 'Admin access required'}), 403

        # Store access info for use in the route
        user['admin_access'] = access
        return f(*args, **kwargs)
    return decorated_function


def require_network_admin(f):
    """Decorator to require network-level admin authentication (full access)."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = session.get('user')
        if not user:
            return jsonify({'error': 'Authentication required'}), 401

        access = get_sabbatical_admin_access(user.get('email', ''))
        if access['level'] != 'network':
            return jsonify({'error': 'Network admin access required'}), 403

        user['admin_access'] = access
        return f(*args, **kwargs)
    return decorated_function


# ============ Public Routes ============

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()

@app.route('/')
def index():
    """Serve the main HTML page."""
    return send_file(os.path.join(SCRIPT_DIR, 'index.html'))


@app.route('/api/applications', methods=['POST'])
def submit_application():
    """Submit a new sabbatical application."""
    try:
        data = request.json

        # Validate required fields
        required_fields = ['employee_name', 'employee_email', 'sabbatical_option',
                          'date_flexibility', 'sabbatical_purpose',
                          'why_now', 'coverage_plan', 'manager_discussion',
                          'ack_one_year', 'ack_no_other_job']

        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400

        # Validate acknowledgments
        if not data.get('ack_one_year') or not data.get('ack_no_other_job'):
            return jsonify({'error': 'You must acknowledge all required statements'}), 400

        # Generate application ID and timestamps
        application_id = str(uuid.uuid4())[:8].upper()
        submitted_at = datetime.now().isoformat()

        # Build application record
        application = {
            'application_id': application_id,
            'submitted_at': submitted_at,
            'employee_name': data.get('employee_name', ''),
            'employee_email': data.get('employee_email', '').lower(),
            'employee_location': data.get('employee_location', ''),
            'sabbatical_option': data.get('sabbatical_option', ''),
            'preferred_dates': data.get('preferred_dates', ''),
            'start_date': data.get('start_date', ''),
            'end_date': data.get('end_date', ''),
            'date_flexibility': data.get('date_flexibility', ''),
            'flexibility_explanation': data.get('flexibility_explanation', ''),
            'sabbatical_purpose': data.get('sabbatical_purpose', ''),
            'why_now': data.get('why_now', ''),
            'coverage_plan': data.get('coverage_plan', ''),
            'manager_discussion': data.get('manager_discussion', ''),
            'ack_one_year': data.get('ack_one_year', False),
            'ack_no_other_job': data.get('ack_no_other_job', False),
            'additional_notes': data.get('additional_notes', ''),
            'status': 'Submitted',
            'status_updated_at': submitted_at,
            'status_updated_by': 'System',
            'admin_notes': ''
        }

        if append_application(application):
            # Send email notifications
            send_application_confirmation(application)
            send_new_application_alert(application)

            return jsonify({
                'success': True,
                'application_id': application_id
            })
        else:
            return jsonify({'error': 'Failed to save application'}), 500

    except Exception as e:
        logger.error(f"Error submitting application: {e}")
        return jsonify({'error': 'Server error'}), 500


@app.route('/api/applications/lookup', methods=['GET'])
def lookup_applications():
    """Look up applications by email. Requires authentication."""
    # Require authentication
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required. Please sign in to view your application status.'}), 401

    user_email = user.get('email', '').lower()
    is_admin = user.get('is_admin') or user_email in [e.lower() for e in ADMIN_USERS]

    email = request.args.get('email', '').lower().strip()

    if not email:
        return jsonify({'error': 'Email required'}), 400

    # Non-admins can only look up their own applications
    primary_user_email = resolve_email_alias(user_email).lower()
    if not is_admin and email.lower() not in [user_email, primary_user_email]:
        return jsonify({'error': 'You can only view your own application status.'}), 403

    # Resolve email alias to primary email for lookups
    primary_email = resolve_email_alias(email).lower()

    all_applications = read_all_applications()

    # Filter to applications by this email (check both original and primary)
    user_applications = [
        a for a in all_applications
        if a.get('employee_email', '').lower() in [email, primary_email]
    ]

    # Remove admin-only fields
    for a in user_applications:
        a.pop('admin_notes', None)

    return jsonify({
        'applications': user_applications
    })


@app.route('/api/staff/lookup', methods=['GET'])
def lookup_staff():
    """Look up staff info by email and check eligibility."""
    email = request.args.get('email', '').lower().strip()

    if not email:
        return jsonify({'error': 'Email required'}), 400

    # Resolve email alias to primary email for lookups
    primary_email = resolve_email_alias(email).lower()

    # Look up staff in staff_master_list_with_function
    try:
        query = """
        SELECT
            First_Name,
            Last_Name,
            Preferred_First_Name,
            Email_Address,
            Job_Title,
            Location_Name,
            Last_Hire_Date,
            Employment_Status,
            DATE_DIFF(CURRENT_DATE(), DATE(Last_Hire_Date), YEAR) as years_of_service
        FROM `talent-demo-482004.talent_grow_observations.staff_master_list_with_function`
        WHERE LOWER(Email_Address) = @email
        AND (Employment_Status IS NULL OR Employment_Status != 'Terminated')
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", primary_email)
            ]
        )
        results = bq_client.query(query, job_config=job_config).result()

        for row in results:
            # Use preferred name if available, otherwise first name
            display_name = row.Preferred_First_Name or row.First_Name
            full_name = f"{display_name} {row.Last_Name}"
            years = row.years_of_service or 0

            # Calculate eligibility (10+ years required)
            # Test override for system testing
            TEST_ELIGIBLE_EMAILS = ['sshirey@firstlineschools.org']
            is_eligible = years >= 10 or email in TEST_ELIGIBLE_EMAILS

            # Format hire date
            hire_date = row.Last_Hire_Date.strftime('%B %d, %Y') if row.Last_Hire_Date else 'Unknown'

            return jsonify({
                'found': True,
                'name': full_name,
                'first_name': display_name,
                'last_name': row.Last_Name,
                'job_title': row.Job_Title or '',
                'location': row.Location_Name or '',
                'hire_date': hire_date,
                'years_of_service': years,
                'is_eligible': is_eligible,
                'eligibility_message': f"You have {years} years of service at FirstLine Schools." if is_eligible
                    else f"You have {years} years of service. The sabbatical program requires 10+ years of continuous service."
            })

        # Not found in staff list - check previous applications as fallback
        all_applications = read_all_applications()
        for a in all_applications:
            if a.get('employee_email', '').lower() in [email, primary_email]:
                return jsonify({
                    'found': True,
                    'name': a.get('employee_name', ''),
                    'is_eligible': None,  # Can't determine
                    'eligibility_message': 'Unable to verify years of service. Please contact HR.'
                })

        return jsonify({
            'found': False,
            'is_eligible': False,
            'eligibility_message': 'Email not found in staff directory. Please use your @firstlineschools.org email.'
        })

    except Exception as e:
        logger.error(f"Staff lookup error: {e}")
        return jsonify({'error': 'Lookup failed', 'found': False}), 500


@app.route('/api/options', methods=['GET'])
def get_options():
    """Get sabbatical options."""
    return jsonify({'options': list(SABBATICAL_OPTIONS.keys())})


# ============ Conflict Check & Calendar ============

@app.route('/api/conflicts/check', methods=['GET'])
def check_conflicts():
    """Check for sabbatical conflicts at a location during date range."""
    location = request.args.get('location', '').strip()
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    if not location or not start_date or not end_date:
        return jsonify({'error': 'Location and dates required'}), 400

    all_applications = read_all_applications()

    # Filter to approved/pending applications at the same location with overlapping dates
    conflicts = []
    for app in all_applications:
        # Only check approved or in-progress applications
        if app.get('status') not in ['Approved', 'Tentatively Approved', 'Plan Submitted', 'Submitted']:
            continue

        # Check if same location
        if app.get('employee_location', '').lower() != location.lower():
            continue

        # Check for date overlap
        app_start = app.get('start_date', '')
        app_end = app.get('end_date', '')

        if app_start and app_end and start_date and end_date:
            # Check if date ranges overlap
            if not (end_date < app_start or start_date > app_end):
                conflicts.append({
                    'employee_name': app.get('employee_name'),
                    'status': app.get('status'),
                    'start_date': app_start,
                    'end_date': app_end,
                    'preferred_dates': app.get('preferred_dates')
                })

    conflict_count = len(conflicts)

    if conflict_count == 0:
        return jsonify({
            'conflicts': [],
            'count': 0,
            'status': 'ok',
            'message': 'No conflicts found. You would be the first at this location during this time.'
        })
    elif conflict_count == 1:
        return jsonify({
            'conflicts': conflicts,
            'count': 1,
            'status': 'warning',
            'message': f'There is already 1 sabbatical scheduled at {location} during this time. We recommend only 1 per location, but 2 is allowed.'
        })
    else:
        return jsonify({
            'conflicts': conflicts,
            'count': conflict_count,
            'status': 'blocked',
            'message': f'There are already {conflict_count} sabbaticals scheduled at {location} during this time. Maximum of 2 per location is allowed.'
        })


@app.route('/api/calendar', methods=['GET'])
def get_calendar_data():
    """Get sabbatical data for calendar view."""
    all_applications = read_all_applications()

    # Filter to applications with dates that are approved or in-progress
    calendar_data = []
    for app in all_applications:
        if app.get('status') in ['Approved', 'Tentatively Approved', 'Plan Submitted', 'Submitted']:
            calendar_data.append({
                'application_id': app.get('application_id'),
                'employee_name': app.get('employee_name'),
                'employee_location': app.get('employee_location'),
                'sabbatical_option': app.get('sabbatical_option'),
                'start_date': app.get('start_date'),
                'end_date': app.get('end_date'),
                'preferred_dates': app.get('preferred_dates'),
                'status': app.get('status')
            })

    # Also group by location for summary
    by_location = {}
    for app in calendar_data:
        loc = app.get('employee_location') or 'Unknown'
        if loc not in by_location:
            by_location[loc] = []
        by_location[loc].append(app)

    return jsonify({
        'applications': calendar_data,
        'by_location': by_location
    })


# ============ My Sabbatical Routes ============

def ensure_my_sabbatical_tables():
    """Create My Sabbatical tables if they don't exist."""
    try:
        # Checklist items table
        checklist_table = f"{PROJECT_ID}.{DATASET_ID}.checklist_items"
        try:
            bq_client.get_table(checklist_table)
        except:
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("application_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("employee_done", "BOOL"),
                bigquery.SchemaField("employee_done_at", "TIMESTAMP"),
                bigquery.SchemaField("employee_done_by", "STRING"),
                bigquery.SchemaField("manager_done", "BOOL"),
                bigquery.SchemaField("manager_done_at", "TIMESTAMP"),
                bigquery.SchemaField("manager_done_by", "STRING"),
                bigquery.SchemaField("hr_done", "BOOL"),
                bigquery.SchemaField("hr_done_at", "TIMESTAMP"),
                bigquery.SchemaField("hr_done_by", "STRING"),
                bigquery.SchemaField("notes_json", "STRING"),  # JSON array of notes
            ]
            table = bigquery.Table(checklist_table, schema=schema)
            bq_client.create_table(table)
            logger.info(f"Created table {checklist_table}")

        # Coverage assignments table
        coverage_table = f"{PROJECT_ID}.{DATASET_ID}.coverage_assignments"
        try:
            bq_client.get_table(coverage_table)
        except:
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("application_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("responsibility", "STRING"),
                bigquery.SchemaField("covered_by", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("notes", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
                bigquery.SchemaField("updated_at", "TIMESTAMP"),
            ]
            table = bigquery.Table(coverage_table, schema=schema)
            bq_client.create_table(table)
            logger.info(f"Created table {coverage_table}")

        # Messages table
        messages_table = f"{PROJECT_ID}.{DATASET_ID}.messages"
        try:
            bq_client.get_table(messages_table)
        except:
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("application_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("from_email", "STRING"),
                bigquery.SchemaField("from_name", "STRING"),
                bigquery.SchemaField("to_recipient", "STRING"),
                bigquery.SchemaField("message", "STRING"),
                bigquery.SchemaField("sent_at", "TIMESTAMP"),
                bigquery.SchemaField("read", "BOOL"),
            ]
            table = bigquery.Table(messages_table, schema=schema)
            bq_client.create_table(table)
            logger.info(f"Created table {messages_table}")

        # Activity history table
        history_table = f"{PROJECT_ID}.{DATASET_ID}.activity_history"
        try:
            bq_client.get_table(history_table)
        except:
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("application_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("user_email", "STRING"),
                bigquery.SchemaField("user_name", "STRING"),
                bigquery.SchemaField("action", "STRING"),
                bigquery.SchemaField("description", "STRING"),
            ]
            table = bigquery.Table(history_table, schema=schema)
            bq_client.create_table(table)
            logger.info(f"Created table {history_table}")

        # Date change requests table
        date_changes_table = f"{PROJECT_ID}.{DATASET_ID}.date_change_requests"
        try:
            bq_client.get_table(date_changes_table)
        except:
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("application_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("requested_by", "STRING"),
                bigquery.SchemaField("requested_at", "TIMESTAMP"),
                bigquery.SchemaField("old_start_date", "DATE"),
                bigquery.SchemaField("old_end_date", "DATE"),
                bigquery.SchemaField("new_start_date", "DATE"),
                bigquery.SchemaField("new_end_date", "DATE"),
                bigquery.SchemaField("reason", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("manager_approved", "BOOL"),
                bigquery.SchemaField("manager_approved_by", "STRING"),
                bigquery.SchemaField("manager_approved_at", "TIMESTAMP"),
                bigquery.SchemaField("talent_approved", "BOOL"),
                bigquery.SchemaField("talent_approved_by", "STRING"),
                bigquery.SchemaField("talent_approved_at", "TIMESTAMP"),
            ]
            table = bigquery.Table(date_changes_table, schema=schema)
            bq_client.create_table(table)
            logger.info(f"Created table {date_changes_table}")

        return True
    except Exception as e:
        logger.error(f"Error creating My Sabbatical tables: {e}")
        return False


def add_activity(application_id, user_email, user_name, action, description):
    """Add an activity to the history."""
    try:
        history_table = f"{PROJECT_ID}.{DATASET_ID}.activity_history"
        query = f"""
        INSERT INTO `{history_table}` (id, application_id, timestamp, user_email, user_name, action, description)
        VALUES (@id, @application_id, @timestamp, @user_email, @user_name, @action, @description)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", str(uuid.uuid4())[:8]),
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", datetime.now()),
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
                bigquery.ScalarQueryParameter("user_name", "STRING", user_name),
                bigquery.ScalarQueryParameter("action", "STRING", action),
                bigquery.ScalarQueryParameter("description", "STRING", description),
            ]
        )
        bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        logger.error(f"Error adding activity: {e}")


@app.route('/my-sabbatical')
def my_sabbatical_page():
    """Serve the My Sabbatical page."""
    return send_file(os.path.join(SCRIPT_DIR, 'my-sabbatical.html'))


@app.route('/approvals')
def approvals_page():
    """Serve the Approvals page."""
    return send_file(os.path.join(SCRIPT_DIR, 'approvals.html'))


@app.route('/api/my-approvals', methods=['GET'])
def get_my_approvals():
    """Get pending approvals for the current user."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    user_email = user.get('email', '').lower()

    try:
        # Query pending approvals for this user
        approvals_table = f"{PROJECT_ID}.{DATASET_ID}.plan_approvals"

        # Check if table exists
        try:
            bq_client.get_table(approvals_table)
        except:
            return jsonify({'approvals': []})

        query = f"""
        SELECT pa.*, a.employee_name, a.employee_email, a.start_date, a.end_date, a.sabbatical_option
        FROM `{approvals_table}` pa
        JOIN `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` a ON pa.application_id = a.application_id
        WHERE LOWER(pa.approver_email) = @user_email
        AND pa.status = 'Pending'
        ORDER BY pa.created_at DESC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
            ]
        )
        results = list(bq_client.query(query, job_config=job_config).result())

        approvals = []
        for row in results:
            approvals.append({
                'id': row.id,
                'application_id': row.application_id,
                'employee_name': row.employee_name,
                'employee_email': row.employee_email,
                'start_date': row.start_date,
                'end_date': row.end_date,
                'sabbatical_option': row.sabbatical_option,
                'approver_role': row.approver_role,
                'approver_type': row.approver_type,
                'status': row.status,
                'created_at': row.created_at.isoformat() if row.created_at else None,
            })

        return jsonify({'approvals': approvals})
    except Exception as e:
        logger.error(f"Error getting approvals: {e}")
        return jsonify({'error': 'Failed to get approvals'}), 500


@app.route('/api/my-sabbatical', methods=['GET'])
def get_my_sabbatical():
    """Get sabbatical data for the logged-in user or specified employee (admin/supervisor)."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    # Check if admin or supervisor is viewing another employee's sabbatical
    requested_email = request.args.get('email', '').lower()
    user_email = user.get('email', '').lower()
    is_admin = user.get('is_admin') or user_email in [e.lower() for e in ADMIN_USERS]

    if requested_email and requested_email != user_email:
        # Check if user is admin OR a supervisor of the requested employee
        is_supervisor = False
        if not is_admin:
            # Check if current user is in the employee's supervisor chain
            supervisor_chain = get_supervisor_chain(requested_email)
            supervisor_emails = [s.get('email', '').lower() for s in supervisor_chain]
            is_supervisor = user_email in supervisor_emails

        if not is_admin and not is_supervisor:
            return jsonify({'error': 'Access denied - you must be an admin or supervisor to view this employee'}), 403
        email = requested_email
        viewing_as_admin = True  # Flag that we're viewing someone else's data
    else:
        email = user_email
        viewing_as_admin = False

    # Resolve email alias to primary email for lookups
    primary_email = resolve_email_alias(email).lower()
    emails_to_check = [email, primary_email] if email != primary_email else [email]

    ensure_my_sabbatical_tables()

    # Find approved/planning sabbatical for this user
    all_applications = read_all_applications()
    logger.info(f"Looking for sabbatical for email: {email} (primary: {primary_email}), found {len(all_applications)} total applications")

    sabbatical = None
    for app in all_applications:
        app_email = app.get('employee_email', '').lower()
        app_status = app.get('status', '')
        if app_email in emails_to_check:
            logger.info(f"Found matching email: {app_email}, status: {app_status}")
            if app_status in ['Tentatively Approved', 'Plan Submitted', 'Approved', 'Planning', 'Confirmed', 'On Sabbatical', 'Returning', 'Completed']:
                sabbatical = app
                break

    if not sabbatical:
        logger.info(f"No active sabbatical found for {email}")
        return jsonify({'found': False})

    application_id = sabbatical['application_id']

    # Look up years of service from staff table
    try:
        yos_query = """
        SELECT DATE_DIFF(CURRENT_DATE(), DATE(Last_Hire_Date), YEAR) as years_of_service
        FROM `talent-demo-482004.talent_grow_observations.staff_master_list_with_function`
        WHERE LOWER(Email_Address) = @email
        AND Employment_Status IN ('Active', 'Leave of absence')
        LIMIT 1
        """
        yos_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", primary_email)]
        )
        yos_result = list(bq_client.query(yos_query, job_config=yos_config).result())
        if yos_result:
            sabbatical['years_of_service'] = yos_result[0].years_of_service
    except Exception as e:
        logger.error(f"Error looking up years of service: {e}")

    # Get checklist items
    checklist = []
    try:
        query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.checklist_items`
        WHERE application_id = @application_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("application_id", "STRING", application_id)]
        )
        results = bq_client.query(query, job_config=job_config).result()
        for row in results:
            notes = []
            if row.notes_json:
                try:
                    notes = json.loads(row.notes_json)
                except:
                    pass
            checklist.append({
                'task_id': row.task_id,
                'employee_done': row.employee_done or False,
                'manager_done': row.manager_done or False,
                'hr_done': row.hr_done or False,
                'notes': notes
            })
    except Exception as e:
        logger.error(f"Error loading checklist: {e}")

    # Get coverage assignments
    coverage = []
    try:
        query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.coverage_assignments`
        WHERE application_id = @application_id
        ORDER BY created_at
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("application_id", "STRING", application_id)]
        )
        results = bq_client.query(query, job_config=job_config).result()
        for row in results:
            coverage.append({
                'id': row.id,
                'responsibility': row.responsibility,
                'covered_by': row.covered_by,
                'email': row.email or '',
                'status': row.status or 'Pending',
                'notes': row.notes or ''
            })
    except Exception as e:
        logger.error(f"Error loading coverage: {e}")

    # Get plan links
    plan_links = []
    try:
        query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.plan_links`
        WHERE application_id = @application_id
        ORDER BY created_at
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("application_id", "STRING", application_id)]
        )
        results = bq_client.query(query, job_config=job_config).result()
        for row in results:
            plan_links.append({
                'id': row.id,
                'title': row.title,
                'url': row.url,
                'created_at': row.created_at.isoformat() if row.created_at else ''
            })
    except Exception as e:
        logger.error(f"Error loading plan links: {e}")

    # Get messages
    messages = []
    try:
        query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.messages`
        WHERE application_id = @application_id
        ORDER BY sent_at DESC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("application_id", "STRING", application_id)]
        )
        results = bq_client.query(query, job_config=job_config).result()
        for row in results:
            messages.append({
                'id': row.id,
                'from_name': row.from_name,
                'from_email': row.from_email,
                'message': row.message,
                'sent_at': row.sent_at.isoformat() if row.sent_at else '',
                'unread': not row.read
            })
    except Exception as e:
        logger.error(f"Error loading messages: {e}")

    # Get activity history
    history = []
    try:
        query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.activity_history`
        WHERE application_id = @application_id
        ORDER BY timestamp DESC
        LIMIT 50
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("application_id", "STRING", application_id)]
        )
        results = bq_client.query(query, job_config=job_config).result()
        for row in results:
            history.append({
                'timestamp': row.timestamp.isoformat() if row.timestamp else '',
                'description': row.description
            })
    except Exception as e:
        logger.error(f"Error loading history: {e}")

    return jsonify({
        'found': True,
        'sabbatical': sabbatical,
        'checklist': checklist,
        'coverage': coverage,
        'plan_links': plan_links,
        'messages': messages,
        'history': history,
        'viewing_as_admin': viewing_as_admin
    })


@app.route('/api/my-sabbatical/checklist/<task_id>', methods=['PATCH'])
def update_checklist_item(task_id):
    """Update a checklist item."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    email = user.get('email', '').lower()
    primary_email = resolve_email_alias(email).lower()
    emails_to_check = [email, primary_email] if email != primary_email else [email]

    data = request.json
    role = data.get('role')
    checked = data.get('checked', False)

    if role not in ['employee', 'manager', 'hr', 'completed']:
        return jsonify({'error': 'Invalid role'}), 400

    # Map 'completed' to 'employee' for database storage (simplified single checkbox)
    db_role = 'employee' if role == 'completed' else role

    # Find user's sabbatical
    all_applications = read_all_applications()
    application_id = None
    for app in all_applications:
        if app.get('employee_email', '').lower() in emails_to_check:
            if app.get('status') in ['Tentatively Approved', 'Plan Submitted', 'Approved', 'Planning', 'Confirmed', 'On Sabbatical', 'Returning', 'Completed']:
                application_id = app['application_id']
                break

    # Also allow managers/HR to update for any sabbatical they can access
    if not application_id:
        # Check if admin/HR
        if email in [e.lower() for e in ADMIN_USERS]:
            # Get application_id from query param
            application_id = request.args.get('application_id')

    if not application_id:
        return jsonify({'error': 'No sabbatical found'}), 404

    try:
        checklist_table = f"{PROJECT_ID}.{DATASET_ID}.checklist_items"

        # Check if item exists
        query = f"""
        SELECT id FROM `{checklist_table}`
        WHERE application_id = @application_id AND task_id = @task_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
            ]
        )
        results = list(bq_client.query(query, job_config=job_config).result())

        if results:
            # Update existing
            update_query = f"""
            UPDATE `{checklist_table}`
            SET {db_role}_done = @checked,
                {db_role}_done_at = @done_at,
                {db_role}_done_by = @done_by
            WHERE application_id = @application_id AND task_id = @task_id
            """
        else:
            # Insert new
            update_query = f"""
            INSERT INTO `{checklist_table}` (id, application_id, task_id, {db_role}_done, {db_role}_done_at, {db_role}_done_by)
            VALUES (@id, @application_id, @task_id, @checked, @done_at, @done_by)
            """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", str(uuid.uuid4())[:8]),
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
                bigquery.ScalarQueryParameter("checked", "BOOL", checked),
                bigquery.ScalarQueryParameter("done_at", "TIMESTAMP", datetime.now() if checked else None),
                bigquery.ScalarQueryParameter("done_by", "STRING", user.get('name', email) if checked else None),
            ]
        )
        bq_client.query(update_query, job_config=job_config).result()

        # Add activity
        action = 'checked' if checked else 'unchecked'
        add_activity(application_id, email, user.get('name', ''), action, f"{user.get('name', '')} {action} task: {task_id}")

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error updating checklist: {e}")
        return jsonify({'error': 'Failed to update'}), 500


@app.route('/api/my-sabbatical/checklist/<task_id>/notes', methods=['POST'])
def add_checklist_note(task_id):
    """Add a note to a checklist item."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    email = user.get('email', '').lower()
    primary_email = resolve_email_alias(email).lower()
    emails_to_check = [email, primary_email] if email != primary_email else [email]

    data = request.json
    note_text = data.get('text', '').strip()

    if not note_text:
        return jsonify({'error': 'Note text required'}), 400

    # Find user's sabbatical
    all_applications = read_all_applications()
    application_id = None
    for app in all_applications:
        if app.get('employee_email', '').lower() in emails_to_check:
            if app.get('status') in ['Tentatively Approved', 'Plan Submitted', 'Approved', 'Planning', 'Confirmed', 'On Sabbatical', 'Returning', 'Completed']:
                application_id = app['application_id']
                break

    if not application_id:
        return jsonify({'error': 'No sabbatical found'}), 404

    try:
        checklist_table = f"{PROJECT_ID}.{DATASET_ID}.checklist_items"

        # Get existing notes
        query = f"""
        SELECT id, notes_json FROM `{checklist_table}`
        WHERE application_id = @application_id AND task_id = @task_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
            ]
        )
        results = list(bq_client.query(query, job_config=job_config).result())

        new_note = {
            'author': user.get('name', email),
            'text': note_text,
            'timestamp': datetime.now().isoformat()
        }

        if results:
            # Update existing
            existing_notes = []
            if results[0].notes_json:
                try:
                    existing_notes = json.loads(results[0].notes_json)
                except:
                    pass
            existing_notes.append(new_note)

            update_query = f"""
            UPDATE `{checklist_table}`
            SET notes_json = @notes_json
            WHERE application_id = @application_id AND task_id = @task_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                    bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
                    bigquery.ScalarQueryParameter("notes_json", "STRING", json.dumps(existing_notes)),
                ]
            )
        else:
            # Insert new
            update_query = f"""
            INSERT INTO `{checklist_table}` (id, application_id, task_id, notes_json)
            VALUES (@id, @application_id, @task_id, @notes_json)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("id", "STRING", str(uuid.uuid4())[:8]),
                    bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                    bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
                    bigquery.ScalarQueryParameter("notes_json", "STRING", json.dumps([new_note])),
                ]
            )

        bq_client.query(update_query, job_config=job_config).result()

        # Add activity
        add_activity(application_id, email, user.get('name', ''), 'note_added', f"{user.get('name', '')} added note to task: {task_id}")

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error adding note: {e}")
        return jsonify({'error': 'Failed to add note'}), 500


@app.route('/api/my-sabbatical/coverage', methods=['POST'])
def add_coverage():
    """Add a coverage assignment."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    email = user.get('email', '').lower()
    primary_email = resolve_email_alias(email).lower()
    emails_to_check = [email, primary_email] if email != primary_email else [email]

    data = request.json

    # Find user's sabbatical
    all_applications = read_all_applications()
    application_id = None
    for app in all_applications:
        if app.get('employee_email', '').lower() in emails_to_check:
            if app.get('status') in ['Tentatively Approved', 'Plan Submitted', 'Approved', 'Planning', 'Confirmed', 'On Sabbatical', 'Returning', 'Completed']:
                application_id = app['application_id']
                break

    if not application_id:
        return jsonify({'error': 'No sabbatical found'}), 404

    try:
        coverage_table = f"{PROJECT_ID}.{DATASET_ID}.coverage_assignments"
        coverage_id = str(uuid.uuid4())[:8]

        query = f"""
        INSERT INTO `{coverage_table}` (id, application_id, responsibility, covered_by, email, status, notes, created_at, updated_at)
        VALUES (@id, @application_id, @responsibility, @covered_by, @email, @status, @notes, @created_at, @updated_at)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", coverage_id),
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("responsibility", "STRING", data.get('responsibility', '')),
                bigquery.ScalarQueryParameter("covered_by", "STRING", data.get('covered_by', '')),
                bigquery.ScalarQueryParameter("email", "STRING", data.get('email', '')),
                bigquery.ScalarQueryParameter("status", "STRING", 'Pending'),
                bigquery.ScalarQueryParameter("notes", "STRING", data.get('notes', '')),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.now()),
                bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", datetime.now()),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        # Add activity
        add_activity(application_id, email, user.get('name', ''), 'coverage_added',
                    f"{user.get('name', '')} added coverage: {data.get('responsibility')} covered by {data.get('covered_by')}")

        return jsonify({'success': True, 'id': coverage_id})
    except Exception as e:
        logger.error(f"Error adding coverage: {e}")
        return jsonify({'error': 'Failed to add coverage'}), 500


@app.route('/api/my-sabbatical/coverage/<coverage_id>', methods=['PATCH'])
def update_coverage(coverage_id):
    """Update a coverage assignment."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    data = request.json

    try:
        coverage_table = f"{PROJECT_ID}.{DATASET_ID}.coverage_assignments"

        # Build update query
        set_clauses = ["updated_at = @updated_at"]
        params = [
            bigquery.ScalarQueryParameter("coverage_id", "STRING", coverage_id),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", datetime.now()),
        ]

        if 'status' in data:
            set_clauses.append("status = @status")
            params.append(bigquery.ScalarQueryParameter("status", "STRING", data['status']))

        if 'responsibility' in data:
            set_clauses.append("responsibility = @responsibility")
            params.append(bigquery.ScalarQueryParameter("responsibility", "STRING", data['responsibility']))

        if 'covered_by' in data:
            set_clauses.append("covered_by = @covered_by")
            params.append(bigquery.ScalarQueryParameter("covered_by", "STRING", data['covered_by']))

        if 'email' in data:
            set_clauses.append("email = @email")
            params.append(bigquery.ScalarQueryParameter("email", "STRING", data['email']))

        if 'notes' in data:
            set_clauses.append("notes = @notes")
            params.append(bigquery.ScalarQueryParameter("notes", "STRING", data['notes']))

        query = f"""
        UPDATE `{coverage_table}`
        SET {', '.join(set_clauses)}
        WHERE id = @coverage_id
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        bq_client.query(query, job_config=job_config).result()

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error updating coverage: {e}")
        return jsonify({'error': 'Failed to update coverage'}), 500


@app.route('/api/my-sabbatical/coverage/<coverage_id>', methods=['DELETE'])
def delete_coverage(coverage_id):
    """Delete a coverage assignment."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    try:
        coverage_table = f"{PROJECT_ID}.{DATASET_ID}.coverage_assignments"

        query = f"""
        DELETE FROM `{coverage_table}`
        WHERE id = @coverage_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("coverage_id", "STRING", coverage_id),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error deleting coverage: {e}")
        return jsonify({'error': 'Failed to delete coverage'}), 500


# ============ Plan Links Routes ============

@app.route('/api/my-sabbatical/plan-links', methods=['POST'])
def add_plan_link():
    """Add a plan document link."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    data = request.json
    title = data.get('title', '').strip()
    url = data.get('url', '').strip()

    if not title or not url:
        return jsonify({'error': 'Title and URL are required'}), 400

    email = user.get('email', '').lower()
    primary_email = resolve_email_alias(email).lower()
    emails_to_check = [email, primary_email] if email != primary_email else [email]

    try:
        # Find application for this user
        all_applications = read_all_applications()
        application = None
        for app in all_applications:
            if app.get('employee_email', '').lower() in emails_to_check:
                if app.get('status') in ['Tentatively Approved', 'Plan Submitted', 'Approved', 'Planning', 'Confirmed', 'On Sabbatical', 'Returning', 'Completed']:
                    application = app
                    break

        if not application:
            return jsonify({'error': 'No active sabbatical application found'}), 404

        plan_links_table = f"{PROJECT_ID}.{DATASET_ID}.plan_links"

        # Ensure table exists
        try:
            bq_client.get_table(plan_links_table)
        except Exception:
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("application_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("employee_email", "STRING"),
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("url", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
                bigquery.SchemaField("created_by", "STRING"),
            ]
            table = bigquery.Table(plan_links_table, schema=schema)
            bq_client.create_table(table)
            logger.info(f"Created table {plan_links_table}")

        link_id = str(uuid.uuid4())[:8]
        query = f"""
        INSERT INTO `{plan_links_table}` (id, application_id, employee_email, title, url, created_at, created_by)
        VALUES (@id, @application_id, @employee_email, @title, @url, CURRENT_TIMESTAMP(), @created_by)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", link_id),
                bigquery.ScalarQueryParameter("application_id", "STRING", application['application_id']),
                bigquery.ScalarQueryParameter("employee_email", "STRING", primary_email),
                bigquery.ScalarQueryParameter("title", "STRING", title),
                bigquery.ScalarQueryParameter("url", "STRING", url),
                bigquery.ScalarQueryParameter("created_by", "STRING", user.get('email')),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        return jsonify({'success': True, 'id': link_id})
    except Exception as e:
        logger.error(f"Error adding plan link: {e}")
        return jsonify({'error': 'Failed to add plan link'}), 500


@app.route('/api/my-sabbatical/plan-links/<link_id>', methods=['PATCH'])
def update_plan_link(link_id):
    """Update a plan document link."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    data = request.json
    title = data.get('title', '').strip()
    url = data.get('url', '').strip()

    if not title or not url:
        return jsonify({'error': 'Title and URL are required'}), 400

    try:
        plan_links_table = f"{PROJECT_ID}.{DATASET_ID}.plan_links"

        query = f"""
        UPDATE `{plan_links_table}`
        SET title = @title, url = @url
        WHERE id = @link_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("link_id", "STRING", link_id),
                bigquery.ScalarQueryParameter("title", "STRING", title),
                bigquery.ScalarQueryParameter("url", "STRING", url),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error updating plan link: {e}")
        return jsonify({'error': 'Failed to update plan link'}), 500


@app.route('/api/my-sabbatical/plan-links/<link_id>', methods=['DELETE'])
def delete_plan_link(link_id):
    """Delete a plan document link."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    try:
        plan_links_table = f"{PROJECT_ID}.{DATASET_ID}.plan_links"

        query = f"""
        DELETE FROM `{plan_links_table}`
        WHERE id = @link_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("link_id", "STRING", link_id),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error deleting plan link: {e}")
        return jsonify({'error': 'Failed to delete plan link'}), 500


@app.route('/api/my-sabbatical/messages', methods=['POST'])
def send_sabbatical_message():
    """Send a message about a sabbatical."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    email = user.get('email', '').lower()
    primary_email = resolve_email_alias(email).lower()
    emails_to_check = [email, primary_email] if email != primary_email else [email]

    data = request.json

    # Find user's sabbatical
    all_applications = read_all_applications()
    application_id = None
    sabbatical = None
    for app in all_applications:
        if app.get('employee_email', '').lower() in emails_to_check:
            if app.get('status') in ['Tentatively Approved', 'Plan Submitted', 'Approved', 'Planning', 'Confirmed', 'On Sabbatical', 'Returning', 'Completed']:
                application_id = app['application_id']
                sabbatical = app
                break

    if not application_id:
        return jsonify({'error': 'No sabbatical found'}), 404

    try:
        messages_table = f"{PROJECT_ID}.{DATASET_ID}.messages"
        message_id = str(uuid.uuid4())[:8]

        query = f"""
        INSERT INTO `{messages_table}` (id, application_id, from_email, from_name, to_recipient, message, sent_at, read)
        VALUES (@id, @application_id, @from_email, @from_name, @to_recipient, @message, @sent_at, @read)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", message_id),
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("from_email", "STRING", email),
                bigquery.ScalarQueryParameter("from_name", "STRING", user.get('name', '')),
                bigquery.ScalarQueryParameter("to_recipient", "STRING", data.get('recipient', '')),
                bigquery.ScalarQueryParameter("message", "STRING", data.get('message', '')),
                bigquery.ScalarQueryParameter("sent_at", "TIMESTAMP", datetime.now()),
                bigquery.ScalarQueryParameter("read", "BOOL", False),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        # Send email notification to recipient
        recipient_emails = {
            'manager': sabbatical.get('manager_email', ''),  # Would need to store this
            'hr': 'hr@firstlineschools.org',
            'benefits': 'benefits@firstlineschools.org',
            'payroll': 'payroll@firstlineschools.org',
            'talent': 'talent@firstlineschools.org'
        }
        to_email = recipient_emails.get(data.get('recipient'), '')
        if to_email:
            subject = f"Sabbatical Message from {user.get('name', '')} - {sabbatical.get('employee_name', '')}"
            html_body = f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px;">
                <h2>New Sabbatical Message</h2>
                <p><strong>From:</strong> {user.get('name', '')} ({email})</p>
                <p><strong>Regarding:</strong> {sabbatical.get('employee_name', '')}'s Sabbatical</p>
                <div style="background: #f5f5f5; padding: 15px; border-radius: 5px; margin: 15px 0;">
                    {data.get('message', '')}
                </div>
                <p><a href="https://sabbatical-program-965913991496.us-central1.run.app/my-sabbatical">View in Sabbatical Portal</a></p>
            </div>
            """
            send_email(to_email, subject, html_body)

        # Add activity
        add_activity(application_id, email, user.get('name', ''), 'message_sent',
                    f"{user.get('name', '')} sent message to {data.get('recipient')}")

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        return jsonify({'error': 'Failed to send message'}), 500


@app.route('/api/my-sabbatical/date-change', methods=['POST'])
def request_date_change():
    """Request a date change for sabbatical."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    email = user.get('email', '').lower()
    primary_email = resolve_email_alias(email).lower()
    emails_to_check = [email, primary_email] if email != primary_email else [email]

    data = request.json

    # Find user's sabbatical
    all_applications = read_all_applications()
    application_id = None
    sabbatical = None
    for app in all_applications:
        if app.get('employee_email', '').lower() in emails_to_check:
            if app.get('status') in ['Tentatively Approved', 'Approved', 'Planning', 'Confirmed']:
                application_id = app['application_id']
                sabbatical = app
                break

    if not application_id:
        return jsonify({'error': 'No sabbatical found'}), 404

    try:
        date_changes_table = f"{PROJECT_ID}.{DATASET_ID}.date_change_requests"
        request_id = str(uuid.uuid4())[:8]

        # Parse dates
        old_start = datetime.strptime(sabbatical['start_date'], '%Y-%m-%d').date() if sabbatical.get('start_date') else None
        old_end = datetime.strptime(sabbatical['end_date'], '%Y-%m-%d').date() if sabbatical.get('end_date') else None
        new_start = datetime.strptime(data['new_start_date'], '%Y-%m-%d').date() if data.get('new_start_date') else None
        new_end = datetime.strptime(data['new_end_date'], '%Y-%m-%d').date() if data.get('new_end_date') else None

        query = f"""
        INSERT INTO `{date_changes_table}` (
            id, application_id, requested_by, requested_at,
            old_start_date, old_end_date, new_start_date, new_end_date,
            reason, status
        ) VALUES (
            @id, @application_id, @requested_by, @requested_at,
            @old_start_date, @old_end_date, @new_start_date, @new_end_date,
            @reason, @status
        )
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", request_id),
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("requested_by", "STRING", email),
                bigquery.ScalarQueryParameter("requested_at", "TIMESTAMP", datetime.now()),
                bigquery.ScalarQueryParameter("old_start_date", "DATE", old_start),
                bigquery.ScalarQueryParameter("old_end_date", "DATE", old_end),
                bigquery.ScalarQueryParameter("new_start_date", "DATE", new_start),
                bigquery.ScalarQueryParameter("new_end_date", "DATE", new_end),
                bigquery.ScalarQueryParameter("reason", "STRING", data.get('reason', '')),
                bigquery.ScalarQueryParameter("status", "STRING", 'Pending'),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        # Send notification emails
        portal_url = request.host_url.rstrip('/')
        approval_link = f"{portal_url}/approvals?date_change={request_id}"

        subject = f"Sabbatical Date Change Request - {sabbatical.get('employee_name', '')}"
        html_body = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px;">
            <h2 style="color: #1e3a5f;">Sabbatical Date Change Request</h2>
            <p><strong>{sabbatical.get('employee_name', '')}</strong> has requested a date change for their sabbatical.</p>

            <table style="width: 100%; border-collapse: collapse; margin: 15px 0;">
                <tr>
                    <th style="text-align: left; padding: 8px; background: #f5f5f5;">Current Dates</th>
                    <td style="padding: 8px;">{sabbatical.get('start_date', 'TBD')} - {sabbatical.get('end_date', 'TBD')}</td>
                </tr>
                <tr>
                    <th style="text-align: left; padding: 8px; background: #f5f5f5;">Requested Dates</th>
                    <td style="padding: 8px; color: #e47727; font-weight: bold;">{data.get('new_start_date', 'TBD')} - {data.get('new_end_date', 'TBD')}</td>
                </tr>
                <tr>
                    <th style="text-align: left; padding: 8px; background: #f5f5f5;">Reason</th>
                    <td style="padding: 8px;">{data.get('reason', 'No reason provided')}</td>
                </tr>
            </table>

            <p style="margin-top: 20px;">
                <a href="{approval_link}" style="background-color: #1e3a5f; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">
                    Review Date Change Request
                </a>
            </p>
        </div>
        """
        # Get supervisor chain emails for CC (CEO only for her direct reports)
        supervisor_chain = filter_chain_for_notifications(get_supervisor_chain(sabbatical.get('employee_email', '')))
        cc_list = [SABBATICAL_ADMIN_EMAIL] + [s['email'] for s in supervisor_chain if s.get('email')]

        send_email(TALENT_TEAM_EMAIL, subject, html_body, cc_emails=cc_list)

        # Add activity
        add_activity(application_id, email, user.get('name', ''), 'date_change_requested',
                    f"{user.get('name', '')} requested date change: {data.get('new_start_date')} - {data.get('new_end_date')}")

        return jsonify({'success': True, 'request_id': request_id})
    except Exception as e:
        logger.error(f"Error requesting date change: {e}")
        return jsonify({'error': 'Failed to submit request'}), 500


@app.route('/api/admin/date-change-requests', methods=['GET'])
@require_network_admin
def get_date_change_requests():
    """Get all pending date change requests (network admin only)."""

    try:
        date_changes_table = f"{PROJECT_ID}.{DATASET_ID}.date_change_requests"
        query = f"""
        SELECT
            dcr.*,
            a.employee_name,
            a.employee_email,
            a.leave_weeks,
            a.salary_percentage
        FROM `{date_changes_table}` dcr
        JOIN `{PROJECT_ID}.{DATASET_ID}.applications` a ON dcr.application_id = a.application_id
        WHERE dcr.status = 'Pending'
        ORDER BY dcr.requested_at DESC
        """
        results = bq_client.query(query).result()

        requests = []
        for row in results:
            # Compute sabbatical_option from leave_weeks and salary_percentage
            sabbatical_option = f"{row.leave_weeks} Weeks - {row.salary_percentage}% Salary" if row.leave_weeks else "N/A"
            requests.append({
                'id': row.id,
                'application_id': row.application_id,
                'employee_name': row.employee_name,
                'employee_email': row.employee_email,
                'sabbatical_option': sabbatical_option,
                'old_start_date': row.old_start_date.isoformat() if row.old_start_date else None,
                'old_end_date': row.old_end_date.isoformat() if row.old_end_date else None,
                'new_start_date': row.new_start_date.isoformat() if row.new_start_date else None,
                'new_end_date': row.new_end_date.isoformat() if row.new_end_date else None,
                'reason': row.reason,
                'requested_at': row.requested_at.isoformat() if row.requested_at else None,
                'status': row.status
            })

        return jsonify({'requests': requests})
    except Exception as e:
        logger.error(f"Error fetching date change requests: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/date-change-requests/<request_id>', methods=['PATCH'])
@require_network_admin
def process_date_change_request(request_id):
    """Approve or deny a date change request (network admin only)."""
    data = request.json
    action = data.get('action')  # 'approve' or 'deny'

    if action not in ['approve', 'deny']:
        return jsonify({'error': 'Invalid action'}), 400

    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401
    user_email = user.get('email', '')

    try:
        date_changes_table = f"{PROJECT_ID}.{DATASET_ID}.date_change_requests"

        # Get the date change request
        query = f"""
        SELECT * FROM `{date_changes_table}` WHERE id = @request_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("request_id", "STRING", request_id)
            ]
        )
        results = list(bq_client.query(query, job_config=job_config).result())

        if not results:
            return jsonify({'error': 'Request not found'}), 404

        dcr = results[0]

        if action == 'approve':
            # Update the application with new dates
            update_application(dcr.application_id, {
                'start_date': dcr.new_start_date.isoformat() if dcr.new_start_date else None,
                'end_date': dcr.new_end_date.isoformat() if dcr.new_end_date else None
            })

            # Add activity
            add_activity(dcr.application_id, user_email, user.get('name', ''), 'date_change_approved',
                        f"Date change approved by {user.get('name', '')}: {dcr.new_start_date} - {dcr.new_end_date}")

            new_status = 'Approved'
        else:
            # Add activity for denial
            add_activity(dcr.application_id, user_email, user.get('name', ''), 'date_change_denied',
                        f"Date change denied by {user.get('name', '')}")

            new_status = 'Denied'

        # Update the date change request status
        update_query = f"""
        UPDATE `{date_changes_table}`
        SET status = @status, talent_approved = @talent_approved, talent_approved_by = @approved_by, talent_approved_at = @approved_at
        WHERE id = @request_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", new_status),
                bigquery.ScalarQueryParameter("talent_approved", "BOOL", action == 'approve'),
                bigquery.ScalarQueryParameter("approved_by", "STRING", user_email),
                bigquery.ScalarQueryParameter("approved_at", "TIMESTAMP", datetime.now()),
                bigquery.ScalarQueryParameter("request_id", "STRING", request_id)
            ]
        )
        bq_client.query(update_query, job_config=job_config).result()

        # Send notification to employee
        all_applications = read_all_applications()
        sabbatical = next((a for a in all_applications if a['application_id'] == dcr.application_id), None)

        if sabbatical:
            # Get supervisor chain for CC (CEO only for her direct reports)
            supervisor_chain = filter_chain_for_notifications(get_supervisor_chain(sabbatical.get('employee_email', '')))
            cc_list = [SABBATICAL_ADMIN_EMAIL, TALENT_TEAM_EMAIL] + [s['email'] for s in supervisor_chain if s.get('email')]

            portal_url = request.host_url.rstrip('/')
            planning_link = f"{portal_url}/my-sabbatical"

            if action == 'approve':
                subject = f"Sabbatical Date Change Approved - {sabbatical.get('employee_name', '')}"
                html_body = f"""
                <div style="font-family: Arial, sans-serif; max-width: 600px;">
                    <h2 style="color: #22c55e;">Date Change Approved</h2>
                    <p><strong>{sabbatical.get('employee_name', '')}'s</strong> sabbatical date change request has been approved.</p>
                    <p><strong>New Dates:</strong> {dcr.new_start_date} - {dcr.new_end_date}</p>
                    <p>Please continue with sabbatical planning.</p>
                    <p style="margin-top: 20px;">
                        <a href="{planning_link}" style="background-color: #1e3a5f; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">
                            Go to My Sabbatical Plan
                        </a>
                    </p>
                </div>
                """
            else:
                subject = f"Sabbatical Date Change Request Denied - {sabbatical.get('employee_name', '')}"
                html_body = f"""
                <div style="font-family: Arial, sans-serif; max-width: 600px;">
                    <h2 style="color: #ef4444;">Date Change Request Denied</h2>
                    <p><strong>{sabbatical.get('employee_name', '')}'s</strong> sabbatical date change request was not approved at this time.</p>
                    <p>Please contact the Talent team if you have questions.</p>
                    <p style="margin-top: 20px;">
                        <a href="{planning_link}" style="background-color: #1e3a5f; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">
                            Go to My Sabbatical Plan
                        </a>
                    </p>
                </div>
                """
            send_email(sabbatical.get('employee_email', ''), subject, html_body, cc_emails=cc_list)

        return jsonify({'success': True, 'status': new_status})
    except Exception as e:
        logger.error(f"Error processing date change request: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/my-sabbatical/submit-plan', methods=['POST'])
def submit_plan_for_approval():
    """Submit sabbatical plan for final approval."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    email = user.get('email', '').lower()
    primary_email = resolve_email_alias(email).lower()
    emails_to_check = [email, primary_email] if email != primary_email else [email]

    # Find user's sabbatical
    all_applications = read_all_applications()
    sabbatical = None
    for app in all_applications:
        if app.get('employee_email', '').lower() in emails_to_check:
            if app.get('status') == 'Tentatively Approved':
                sabbatical = app
                break

    if not sabbatical:
        return jsonify({'error': 'No tentatively approved sabbatical found'}), 404

    application_id = sabbatical['application_id']

    try:
        # Get required approvers - use primary email for supervisor chain lookup
        approvers = get_required_approvers(primary_email)

        # Create plan_approvals table entry for tracking
        approvals_table = f"{PROJECT_ID}.{DATASET_ID}.plan_approvals"

        # Ensure table exists (create if needed)
        try:
            bq_client.get_table(approvals_table)
        except:
            schema = [
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("application_id", "STRING"),
                bigquery.SchemaField("approver_email", "STRING"),
                bigquery.SchemaField("approver_name", "STRING"),
                bigquery.SchemaField("approver_role", "STRING"),
                bigquery.SchemaField("approver_type", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("approved_at", "TIMESTAMP"),
                bigquery.SchemaField("notes", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ]
            table = bigquery.Table(approvals_table, schema=schema)
            bq_client.create_table(table)
            logger.info(f"Created table {approvals_table}")

        # Check if approvals already exist for this application (prevent duplicates)
        existing_query = f"""
        SELECT COUNT(*) as cnt FROM `{approvals_table}`
        WHERE application_id = @application_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
            ]
        )
        existing = list(bq_client.query(existing_query, job_config=job_config).result())[0].cnt
        if existing > 0:
            return jsonify({'error': 'Plan has already been submitted for approval'}), 400

        # Insert approval records for each approver
        for approver in approvers:
            approval_id = str(uuid.uuid4())[:8]
            query = f"""
            INSERT INTO `{approvals_table}` (id, application_id, approver_email, approver_name, approver_role, approver_type, status, created_at)
            VALUES (@id, @application_id, @approver_email, @approver_name, @approver_role, @approver_type, 'Pending', @created_at)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("id", "STRING", approval_id),
                    bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                    bigquery.ScalarQueryParameter("approver_email", "STRING", approver['email']),
                    bigquery.ScalarQueryParameter("approver_name", "STRING", approver['name']),
                    bigquery.ScalarQueryParameter("approver_role", "STRING", approver['role']),
                    bigquery.ScalarQueryParameter("approver_type", "STRING", approver['type']),
                    bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.now()),
                ]
            )
            bq_client.query(query, job_config=job_config).result()

        # Update application status to "Plan Submitted"
        update_application(application_id, {'status': 'Plan Submitted'})

        # Send notification to all approvers
        approver_list = ', '.join([a['name'] for a in approvers])
        subject = f"Sabbatical Plan Approval Required - {sabbatical.get('employee_name', '')}"
        for approver in approvers:
            html_body = f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px;">
                <div style="background-color: #6B46C1; padding: 20px; text-align: center;">
                    <h1 style="color: white; margin: 0;">Sabbatical Plan Approval</h1>
                </div>
                <div style="padding: 20px; background-color: #f8f9fa;">
                    <p>Hi {approver['name']},</p>
                    <p><strong>{sabbatical.get('employee_name', '')}</strong> has submitted their sabbatical plan for final approval.</p>

                    <div style="background-color: white; border-radius: 8px; padding: 15px; margin: 20px 0;">
                        <p style="margin: 5px 0;"><strong>Employee:</strong> {sabbatical.get('employee_name', '')}</p>
                        <p style="margin: 5px 0;"><strong>Dates:</strong> {sabbatical.get('start_date', 'TBD')} - {sabbatical.get('end_date', 'TBD')}</p>
                        <p style="margin: 5px 0;"><strong>Your Role:</strong> {approver['role']}</p>
                    </div>

                    <p>Please review the plan and provide your approval.</p>

                    <div style="text-align: center; margin: 20px 0;">
                        <a href="https://sabbatical-program-965913991496.us-central1.run.app/my-sabbatical?email={email}"
                           style="display: inline-block; background-color: #6B46C1; color: white; padding: 12px 30px;
                                  text-decoration: none; border-radius: 5px; font-weight: bold;">
                            Review & Approve Plan
                        </a>
                    </div>

                    <p style="color: #666; font-size: 0.9em;">Other approvers: {approver_list}</p>
                </div>
            </div>
            """
            send_email(approver['email'], subject, html_body)

        # Add activity
        add_activity(application_id, email, user.get('name', ''), 'plan_submitted',
                    f"Plan submitted for final approval. Awaiting: {approver_list}")

        return jsonify({'success': True, 'approvers': approvers})
    except Exception as e:
        logger.error(f"Error submitting plan: {e}")
        return jsonify({'error': 'Failed to submit plan'}), 500


@app.route('/api/my-sabbatical/approve-plan', methods=['POST'])
def approve_plan():
    """Approve a sabbatical plan (for managers, Talent, HR)."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    approver_email = user.get('email', '').lower()
    data = request.json
    application_id = data.get('application_id')
    notes = data.get('notes', '')

    if not application_id:
        return jsonify({'error': 'Application ID required'}), 400

    try:
        approvals_table = f"{PROJECT_ID}.{DATASET_ID}.plan_approvals"

        # Update this approver's record
        query = f"""
        UPDATE `{approvals_table}`
        SET status = 'Approved', approved_at = @approved_at, notes = @notes
        WHERE application_id = @application_id AND LOWER(approver_email) = LOWER(@approver_email)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("approver_email", "STRING", approver_email),
                bigquery.ScalarQueryParameter("approved_at", "TIMESTAMP", datetime.now()),
                bigquery.ScalarQueryParameter("notes", "STRING", notes),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        # Check if all approvals are complete
        check_query = f"""
        SELECT
            COUNT(*) as total,
            COUNTIF(status = 'Approved') as approved
        FROM `{approvals_table}`
        WHERE application_id = @application_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id)
            ]
        )
        results = list(bq_client.query(check_query, job_config=job_config).result())

        if results:
            total = results[0].total
            approved = results[0].approved

            # Get application details
            sabbatical = get_application_by_id(application_id)

            if approved == total and sabbatical:
                # All approvals complete - grant final approval!
                update_application(application_id, {'status': 'Approved'})

                # Get all approvers for notification
                approvers_query = f"""
                SELECT approver_email, approver_name, approver_role
                FROM `{approvals_table}`
                WHERE application_id = @application_id
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("application_id", "STRING", application_id)
                    ]
                )
                approvers = list(bq_client.query(approvers_query, job_config=job_config).result())
                approver_names = [a.approver_name for a in approvers]

                # Get supervisor chain for CC (CEO only for her direct reports)
                supervisor_chain = filter_chain_for_notifications(get_supervisor_chain(sabbatical.get('employee_email', '')))
                supervisor_cc = [s.get('email') for s in supervisor_chain if s.get('email')]

                # Determine the plan type display
                leave_weeks = sabbatical.get('leave_weeks', 8)
                salary_pct = sabbatical.get('salary_percentage', 100)
                plan_type = f"{leave_weeks} Week Plan at {salary_pct}% Salary"

                # Format dates nicely
                start_date = sabbatical.get('start_date', 'TBD')
                end_date = sabbatical.get('end_date', 'TBD')

                # Get employee's first name for a personal touch
                employee_name = sabbatical.get('employee_name', '')
                first_name = employee_name.split()[0] if employee_name else 'Team Member'

                subject = f"Congratulations! Your Sabbatical is Officially Approved - {employee_name}"
                html_body = f"""
                <div style="font-family: 'Open Sans', Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                    <div style="background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%); padding: 40px 20px; text-align: center;">
                        <div style="font-size: 48px; margin-bottom: 10px;">🎉</div>
                        <h1 style="color: white; margin: 0; font-size: 28px;">Congratulations, {first_name}!</h1>
                        <p style="color: rgba(255,255,255,0.9); margin: 10px 0 0 0; font-size: 16px;">Your sabbatical has been officially approved!</p>
                    </div>
                    <div style="padding: 30px; background-color: #f8f9fa;">
                        <p style="font-size: 16px; line-height: 1.6;">Dear {first_name},</p>

                        <p style="font-size: 16px; line-height: 1.6;">We are thrilled to inform you that your sabbatical plan has received <strong>final approval</strong> from all parties! This is a wonderful milestone, and we want to thank you for all the thoughtful planning and preparation you've put into making this possible.</p>

                        <p style="font-size: 16px; line-height: 1.6;">Your dedication to FirstLine Schools over the years has earned you this well-deserved time for rest, renewal, and personal growth. We hope this sabbatical brings you everything you're looking for.</p>

                        <div style="background-color: #002f60; border-radius: 12px; padding: 25px; margin: 25px 0; color: white;">
                            <h2 style="margin: 0 0 20px 0; color: white; font-size: 18px; text-align: center;">Your Approved Sabbatical Details</h2>
                            <table style="width: 100%; color: white; border-collapse: collapse;">
                                <tr>
                                    <td style="padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.2);"><strong>Duration:</strong></td>
                                    <td style="padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.2); text-align: right; font-size: 18px; font-weight: bold;">{leave_weeks} Weeks</td>
                                </tr>
                                <tr>
                                    <td style="padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.2);"><strong>Salary:</strong></td>
                                    <td style="padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.2); text-align: right; font-size: 18px; font-weight: bold;">{salary_pct}%</td>
                                </tr>
                                <tr style="background-color: rgba(228,119,39,0.3);">
                                    <td style="padding: 15px 10px; border-radius: 8px 0 0 0;"><strong>Start Date:</strong></td>
                                    <td style="padding: 15px 10px; text-align: right; font-size: 20px; font-weight: bold; border-radius: 0 8px 0 0;">{start_date}</td>
                                </tr>
                                <tr style="background-color: rgba(228,119,39,0.3);">
                                    <td style="padding: 15px 10px; border-radius: 0 0 0 8px;"><strong>End Date:</strong></td>
                                    <td style="padding: 15px 10px; text-align: right; font-size: 20px; font-weight: bold; border-radius: 0 0 8px 0;">{end_date}</td>
                                </tr>
                            </table>
                        </div>

                        <div style="background-color: white; border-radius: 8px; padding: 20px; margin: 20px 0; border-left: 4px solid #22c55e;">
                            <p style="margin: 0 0 10px 0; font-weight: bold; color: #002f60;">What happens next?</p>
                            <ul style="margin: 0; padding-left: 20px; color: #666;">
                                <li style="margin-bottom: 8px;">HR, Benefits, and Payroll have been notified and will update your records</li>
                                <li style="margin-bottom: 8px;">Continue any final handoff preparations with your coverage team</li>
                                <li style="margin-bottom: 8px;">Enjoy your well-earned time away!</li>
                            </ul>
                        </div>

                        <p style="font-size: 16px; line-height: 1.6;">Thank you for your continued commitment to our students and community. We look forward to welcoming you back refreshed and renewed!</p>

                        <p style="font-size: 16px; line-height: 1.6; margin-bottom: 5px;">Warm regards,</p>
                        <p style="font-size: 16px; line-height: 1.6; margin-top: 0;"><strong>The FirstLine Schools Talent Team</strong></p>

                        <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 25px 0;">

                        <p style="font-size: 12px; color: #888; margin: 0;"><strong>Approved by:</strong> {', '.join(approver_names)}</p>
                        <p style="font-size: 12px; color: #888; margin: 5px 0 0 0;">CC: HR, Benefits, Payroll, and Management Chain</p>
                    </div>
                    <div style="background-color: #002f60; padding: 20px; text-align: center;">
                        <p style="color: white; margin: 0; font-size: 14px;">FirstLine Schools - Education For Life</p>
                    </div>
                </div>
                """

                # Send TO the employee, CC everyone else
                cc_list = [HR_EMAIL, BENEFITS_EMAIL, PAYROLL_EMAIL, SABBATICAL_ADMIN_EMAIL] + supervisor_cc
                employee_email = sabbatical.get('employee_email')

                if employee_email:
                    send_email(employee_email, subject, html_body, cc_emails=cc_list)

                # Add activity
                add_activity(application_id, approver_email, user.get('name', ''), 'final_approval',
                            f"Final approval granted. All {total} approvers signed off.")

                return jsonify({'success': True, 'final_approval': True, 'total': total, 'approved': approved})

        # Add activity for this approval
        add_activity(application_id, approver_email, user.get('name', ''), 'approval_given',
                    f"{user.get('name', approver_email)} approved the plan")

        return jsonify({'success': True, 'final_approval': False, 'total': total, 'approved': approved})
    except Exception as e:
        logger.error(f"Error approving plan: {e}")
        return jsonify({'error': 'Failed to approve plan'}), 500


@app.route('/api/my-sabbatical/request-changes', methods=['POST'])
def request_changes():
    """Request changes to a sabbatical plan (for approvers)."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    data = request.get_json()
    application_id = data.get('application_id')
    comments = data.get('comments', '')

    if not application_id:
        return jsonify({'error': 'Application ID required'}), 400

    approver_email = user.get('email', '').lower()

    try:
        approvals_table = f"{PROJECT_ID}.{DATASET_ID}.plan_approvals"

        # Update this approver's record to "Changes Requested"
        query = f"""
        UPDATE `{approvals_table}`
        SET status = 'Changes Requested', notes = @notes, approved_at = @now
        WHERE application_id = @application_id
        AND LOWER(approver_email) = @approver_email
        AND status = 'Pending'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("approver_email", "STRING", approver_email),
                bigquery.ScalarQueryParameter("notes", "STRING", comments),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", datetime.now()),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        # Get application details
        sabbatical = get_application_by_id(application_id)

        # Notify the employee
        if sabbatical:
            subject = f"Changes Requested - Sabbatical Plan"
            html_body = f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px;">
                <div style="background-color: #eab308; padding: 20px; text-align: center;">
                    <h1 style="color: white; margin: 0;">Changes Requested</h1>
                </div>
                <div style="padding: 20px; background-color: #f8f9fa;">
                    <p>Hi {sabbatical.get('employee_name', '')},</p>
                    <p><strong>{user.get('name', approver_email)}</strong> has requested changes to your sabbatical plan.</p>

                    <div style="background-color: white; border-radius: 8px; padding: 15px; margin: 20px 0;">
                        <p style="margin: 5px 0;"><strong>Reviewer:</strong> {user.get('name', approver_email)}</p>
                        <p style="margin: 5px 0;"><strong>Comments:</strong></p>
                        <p style="margin: 5px 0; font-style: italic; color: #666;">"{comments}"</p>
                    </div>

                    <p>Please review the feedback and update your plan, then resubmit for approval.</p>

                    <div style="text-align: center; margin: 20px 0;">
                        <a href="https://sabbatical-program-965913991496.us-central1.run.app/my-sabbatical"
                           style="display: inline-block; background-color: #6B46C1; color: white; padding: 12px 30px;
                                  text-decoration: none; border-radius: 5px; font-weight: bold;">
                            View Your Plan
                        </a>
                    </div>
                </div>
            </div>
            """
            send_email(sabbatical.get('employee_email'), subject, html_body)

            # Add activity
            add_activity(application_id, approver_email, user.get('name', ''), 'changes_requested',
                        f"{user.get('name', approver_email)} requested changes: {comments}")

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error requesting changes: {e}")
        return jsonify({'error': 'Failed to request changes'}), 500


@app.route('/api/my-sabbatical/resubmit-plan', methods=['POST'])
def resubmit_plan():
    """Resubmit sabbatical plan after making changes."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    data = request.get_json()
    application_id = data.get('application_id')

    if not application_id:
        return jsonify({'error': 'Application ID required'}), 400

    try:
        approvals_table = f"{PROJECT_ID}.{DATASET_ID}.plan_approvals"

        # Reset all approval statuses to Pending
        query = f"""
        UPDATE `{approvals_table}`
        SET status = 'Pending', approved_at = NULL
        WHERE application_id = @application_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        # Get application and approvers
        sabbatical = get_application_by_id(application_id)

        # Get all approvers
        approvers_query = f"""
        SELECT approver_email, approver_name, approver_role
        FROM `{approvals_table}`
        WHERE application_id = @application_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id),
            ]
        )
        approvers = list(bq_client.query(approvers_query, job_config=job_config).result())

        # Notify all approvers
        for approver in approvers:
            subject = f"Plan Resubmitted - {sabbatical.get('employee_name', '')}"
            html_body = f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px;">
                <div style="background-color: #6B46C1; padding: 20px; text-align: center;">
                    <h1 style="color: white; margin: 0;">Plan Resubmitted</h1>
                </div>
                <div style="padding: 20px; background-color: #f8f9fa;">
                    <p>Hi {approver.approver_name},</p>
                    <p><strong>{sabbatical.get('employee_name', '')}</strong> has updated and resubmitted their sabbatical plan for approval.</p>

                    <div style="background-color: white; border-radius: 8px; padding: 15px; margin: 20px 0;">
                        <p style="margin: 5px 0;"><strong>Employee:</strong> {sabbatical.get('employee_name', '')}</p>
                        <p style="margin: 5px 0;"><strong>Dates:</strong> {sabbatical.get('start_date', 'TBD')} - {sabbatical.get('end_date', 'TBD')}</p>
                        <p style="margin: 5px 0;"><strong>Your Role:</strong> {approver.approver_role}</p>
                    </div>

                    <p>Please review the updated plan and provide your approval.</p>

                    <div style="text-align: center; margin: 20px 0;">
                        <a href="https://sabbatical-program-965913991496.us-central1.run.app/my-sabbatical?email={sabbatical.get('employee_email', '')}"
                           style="display: inline-block; background-color: #6B46C1; color: white; padding: 12px 30px;
                                  text-decoration: none; border-radius: 5px; font-weight: bold;">
                            Review & Approve
                        </a>
                    </div>
                </div>
            </div>
            """
            send_email(approver.approver_email, subject, html_body)

        # Add activity
        add_activity(application_id, user.get('email', ''), user.get('name', ''), 'plan_resubmitted',
                    f"{user.get('name', '')} resubmitted the plan for approval")

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error resubmitting plan: {e}")
        return jsonify({'error': 'Failed to resubmit plan'}), 500


@app.route('/api/my-sabbatical/approval-status', methods=['GET'])
def get_approval_status():
    """Get current approval status for a sabbatical plan."""
    user = session.get('user')
    if not user:
        return jsonify({'error': 'Authentication required'}), 401

    application_id = request.args.get('application_id')
    if not application_id:
        return jsonify({'error': 'Application ID required'}), 400

    try:
        approvals_table = f"{PROJECT_ID}.{DATASET_ID}.plan_approvals"

        query = f"""
        SELECT approver_email, approver_name, approver_role, approver_type, status, approved_at, notes
        FROM `{approvals_table}`
        WHERE application_id = @application_id
        ORDER BY approver_type, approver_role
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id)
            ]
        )
        results = bq_client.query(query, job_config=job_config).result()

        approvals = []
        for row in results:
            approvals.append({
                'email': row.approver_email,
                'name': row.approver_name,
                'role': row.approver_role,
                'type': row.approver_type,
                'status': row.status,
                'approved_at': row.approved_at.isoformat() if row.approved_at else None,
                'notes': row.notes
            })

        return jsonify({'approvals': approvals})
    except Exception as e:
        logger.error(f"Error getting approval status: {e}")
        return jsonify({'approvals': []})


# ============ Auth Routes ============

@app.route('/login')
def login():
    """Initiate Google OAuth."""
    if not google:
        return jsonify({'error': 'OAuth not configured'}), 500
    # Remember where to redirect after login
    next_url = request.args.get('redirect', '/')
    session['login_next'] = next_url
    redirect_uri = url_for('auth_callback', _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route('/auth/callback')
def auth_callback():
    """Handle OAuth callback."""
    if not google:
        return jsonify({'error': 'OAuth not configured'}), 500

    try:
        token = google.authorize_access_token()
        user_info = token.get('userinfo')

        if user_info:
            email = user_info.get('email', '').lower()
            session['user'] = {
                'email': user_info.get('email'),
                'name': user_info.get('name'),
                'picture': user_info.get('picture'),
                'is_admin': email in [e.lower() for e in ADMIN_USERS]
            }

        # Redirect back to where the user came from
        next_url = session.pop('login_next', '/')
        return redirect(next_url)
    except Exception as e:
        logger.error(f"OAuth error: {e}")
        return redirect('/?error=auth_failed')


@app.route('/logout')
def logout():
    """Clear session."""
    session.clear()
    return redirect('/')


@app.route('/api/auth/status')
def auth_status():
    """Check authentication status."""
    user = session.get('user')
    if user:
        access = get_sabbatical_admin_access(user.get('email', ''))
        is_admin = access['level'] != 'none'  # Any admin level counts
        return jsonify({
            'authenticated': True,
            'is_admin': is_admin,
            'admin_access': access,  # 'network', 'school', or 'none'
            'user': user
        })
    return jsonify({'authenticated': False, 'is_admin': False, 'admin_access': {'level': 'none'}})


# ============ Admin Routes ============

@app.route('/api/admin/applications', methods=['GET'])
@require_admin
def get_all_applications():
    """Get applications based on admin access level."""
    user = session.get('user', {})
    access = user.get('admin_access', get_sabbatical_admin_access(user.get('email', '')))

    applications = read_all_applications()

    # Filter by school if school-level admin
    if access['level'] == 'school':
        school = access.get('school', '').lower()
        applications = [
            a for a in applications
            if (a.get('employee_location', '') or '').lower() == school
        ]

    return jsonify({'applications': applications, 'access': access})


@app.route('/api/admin/applications/<application_id>', methods=['PATCH'])
@require_network_admin
def update_application_status(application_id):
    """Update an application (network admin only)."""
    try:
        data = request.json
        user = session.get('user', {})

        # Get current application data for email notification
        current_application = get_application_by_id(application_id)
        old_status = current_application.get('status') if current_application else None

        updates = {}
        new_status = None
        notes = data.get('admin_notes', '')

        # Handle status update
        if 'status' in data:
            new_status = data['status']
            if new_status not in STATUS_VALUES:
                return jsonify({'error': 'Invalid status'}), 400

            updates['status'] = new_status
            updates['status_updated_at'] = datetime.now().isoformat()
            updates['status_updated_by'] = user.get('email', 'Unknown')

        # Handle other fields
        if 'admin_notes' in data:
            updates['admin_notes'] = data['admin_notes']

        if update_application(application_id, updates):
            # Send status update email if status changed
            if new_status and old_status and new_status != old_status and current_application:
                send_status_update(current_application, old_status, new_status, user.get('email', 'Unknown'), notes)

            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Application not found'}), 404

    except Exception as e:
        logger.error(f"Error updating application: {e}")
        return jsonify({'error': 'Server error'}), 500


@app.route('/api/admin/applications/<application_id>/resend-confirmation', methods=['POST'])
@require_network_admin
def resend_confirmation_email(application_id):
    """Resend confirmation emails for an application (network admin only)."""
    try:
        application = get_application_by_id(application_id)
        if not application:
            return jsonify({'error': 'Application not found'}), 404

        # Send both emails
        send_application_confirmation(application)
        send_new_application_alert(application)

        return jsonify({
            'success': True,
            'message': f"Confirmation emails sent for {application.get('employee_name', 'Unknown')}"
        })
    except Exception as e:
        logger.error(f"Error resending confirmation: {e}")
        return jsonify({'error': 'Failed to send emails'}), 500


@app.route('/api/admin/applications/<application_id>', methods=['DELETE'])
@require_network_admin
def delete_application_admin(application_id):
    """Delete an application (network admin only)."""
    try:
        user = session.get('user', {})

        # Get application info for logging
        application = get_application_by_id(application_id)
        if not application:
            return jsonify({'error': 'Application not found'}), 404

        # Delete from BigQuery
        query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE application_id = @application_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_id)
            ]
        )
        bq_client.query(query, job_config=job_config).result()

        logger.info(f"Application {application_id} for {application.get('employee_name')} deleted by {user.get('email')}")

        return jsonify({'success': True})

    except Exception as e:
        logger.error(f"Error deleting application: {e}")
        return jsonify({'error': 'Server error'}), 500


@app.route('/api/admin/stats', methods=['GET'])
@require_admin
def get_stats():
    """Get dashboard statistics based on admin access level."""
    user = session.get('user', {})
    access = user.get('admin_access', get_sabbatical_admin_access(user.get('email', '')))

    applications = read_all_applications()

    # Filter by school if school-level admin
    if access['level'] == 'school':
        school = access.get('school', '').lower()
        applications = [
            a for a in applications
            if (a.get('employee_location', '') or '').lower() == school
        ]

    total = len(applications)
    submitted = len([a for a in applications if a.get('status') == 'Submitted'])
    tentatively_approved = len([a for a in applications if a.get('status') == 'Tentatively Approved'])
    plan_submitted = len([a for a in applications if a.get('status') == 'Plan Submitted'])
    approved = len([a for a in applications if a.get('status') == 'Approved'])
    completed = len([a for a in applications if a.get('status') == 'Completed'])
    denied = len([a for a in applications if a.get('status') == 'Denied'])
    withdrawn = len([a for a in applications if a.get('status') == 'Withdrawn'])

    return jsonify({
        'total': total,
        'submitted': submitted,
        'tentatively_approved': tentatively_approved,
        'plan_submitted': plan_submitted,
        'approved': approved,
        'completed': completed,
        'denied': denied,
        'withdrawn': withdrawn,
        'access': access
    })


@app.route('/api/statuses', methods=['GET'])
def get_statuses():
    """Get list of valid status values."""
    return jsonify({'statuses': STATUS_VALUES})


# ============ Health Check ============

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)
