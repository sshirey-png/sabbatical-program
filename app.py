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
HR_EMAIL = 'hr@firstlineschools.org'

# Admin users who can access the admin panel
ADMIN_USERS = [
    'sshirey@firstlineschools.org',
    'brichardson@firstlineschools.org',
    'talent@firstlineschools.org',
    'hr@firstlineschools.org',
    'awatts@firstlineschools.org',
    'jlombas@firstlineschools.org',
    'tcole@firstlineschools.org'
]

# Status values and their display order
STATUS_VALUES = [
    'Submitted',
    'Under Review',
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
                <p style="margin: 5px 0;"><strong>Preferred Dates:</strong> {application['preferred_dates']}</p>
            </div>

            <p><strong>What's next?</strong></p>
            <ul>
                <li>Your application will be reviewed by the Talent team</li>
                <li>We may reach out if we need additional information</li>
                <li>You'll receive an email once a decision has been made</li>
            </ul>

            <p>You can check your application status anytime at the Sabbatical Program portal.</p>

            <p style="color: #666; font-size: 0.9em; margin-top: 30px;">Questions? Contact talent@firstlineschools.org</p>
        </div>
        <div style="background-color: #002f60; padding: 15px; text-align: center;">
            <p style="color: white; margin: 0; font-size: 0.9em;">FirstLine Schools - Education For Life</p>
        </div>
    </div>
    """
    send_email(application['employee_email'], subject, html_body)


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
                <p style="margin: 5px 0;"><strong>Preferred Dates:</strong> {application['preferred_dates']}</p>
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
    send_email(TALENT_TEAM_EMAIL, subject, html_body)


def send_status_update(application, old_status, new_status, updated_by, notes=''):
    """Send status update email to applicant."""
    status_messages = {
        'Under Review': "Your sabbatical application is now being reviewed by our team.",
        'Approved': "Congratulations! Your sabbatical application has been APPROVED! We'll be in touch with next steps.",
        'Denied': f"After careful consideration, we are unable to approve your sabbatical request at this time.{' Notes: ' + notes if notes else ''}",
        'Withdrawn': "Your sabbatical application has been withdrawn as requested."
    }

    message = status_messages.get(new_status, f"Your application status has been updated to: {new_status}")

    # Choose color based on status
    if new_status == 'Approved':
        status_color = '#22c55e'  # Green
    elif new_status in ['Denied', 'Withdrawn']:
        status_color = '#ef4444'  # Red
    else:
        status_color = '#e47727'  # Orange

    subject = f"Sabbatical Application Update - {new_status}"
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

            <div style="background-color: white; border-radius: 8px; padding: 15px; margin: 20px 0;">
                <p style="margin: 5px 0;"><strong>Application ID:</strong> {application['application_id']}</p>
                <p style="margin: 5px 0;"><strong>Preferred Dates:</strong> {application['preferred_dates']}</p>
            </div>

            <p style="color: #666; font-size: 0.9em; margin-top: 30px;">Questions? Contact talent@firstlineschools.org</p>
        </div>
        <div style="background-color: #002f60; padding: 15px; text-align: center;">
            <p style="color: white; margin: 0; font-size: 0.9em;">FirstLine Schools - Education For Life</p>
        </div>
    </div>
    """
    send_email(application['employee_email'], subject, html_body)


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
    return {
        'application_id': row.application_id,
        'submitted_at': row.submitted_at.isoformat() if row.submitted_at else '',
        'employee_name': row.employee_name or '',
        'employee_email': row.employee_email or '',
        'employee_location': getattr(row, 'employee_location', '') or '',
        'sabbatical_option': row.sabbatical_option or '',
        'preferred_dates': row.preferred_dates or '',
        'start_date': row.start_date.isoformat() if getattr(row, 'start_date', None) else '',
        'end_date': row.end_date.isoformat() if getattr(row, 'end_date', None) else '',
        'date_flexibility': row.date_flexibility or '',
        'flexibility_explanation': row.flexibility_explanation or '',
        'sabbatical_purpose': row.sabbatical_purpose or '',
        'why_now': row.why_now or '',
        'coverage_plan': row.coverage_plan or '',
        'manager_discussion': row.manager_discussion or '',
        'ack_one_year': row.ack_one_year if hasattr(row, 'ack_one_year') else False,
        'ack_no_other_job': row.ack_no_other_job if hasattr(row, 'ack_no_other_job') else False,
        'additional_notes': row.additional_notes or '',
        'status': row.status or '',
        'status_updated_at': row.status_updated_at.isoformat() if row.status_updated_at else '',
        'status_updated_by': row.status_updated_by or '',
        'admin_notes': row.admin_notes or ''
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
            application_id, submitted_at, employee_name, employee_email, employee_location,
            sabbatical_option, preferred_dates, start_date, end_date,
            date_flexibility, flexibility_explanation,
            sabbatical_purpose, why_now, coverage_plan, manager_discussion,
            ack_one_year, ack_no_other_job, additional_notes,
            status, status_updated_at, status_updated_by, admin_notes
        ) VALUES (
            @application_id, @submitted_at, @employee_name, @employee_email, @employee_location,
            @sabbatical_option, @preferred_dates, @start_date, @end_date,
            @date_flexibility, @flexibility_explanation,
            @sabbatical_purpose, @why_now, @coverage_plan, @manager_discussion,
            @ack_one_year, @ack_no_other_job, @additional_notes,
            @status, @status_updated_at, @status_updated_by, @admin_notes
        )
        """

        submitted_at = datetime.fromisoformat(application_data['submitted_at']) if application_data.get('submitted_at') else datetime.now()
        status_updated_at = datetime.fromisoformat(application_data['status_updated_at']) if application_data.get('status_updated_at') else datetime.now()

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

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("application_id", "STRING", application_data.get('application_id', '')),
                bigquery.ScalarQueryParameter("submitted_at", "TIMESTAMP", submitted_at),
                bigquery.ScalarQueryParameter("employee_name", "STRING", application_data.get('employee_name', '')),
                bigquery.ScalarQueryParameter("employee_email", "STRING", application_data.get('employee_email', '')),
                bigquery.ScalarQueryParameter("employee_location", "STRING", application_data.get('employee_location', '')),
                bigquery.ScalarQueryParameter("sabbatical_option", "STRING", application_data.get('sabbatical_option', '')),
                bigquery.ScalarQueryParameter("preferred_dates", "STRING", application_data.get('preferred_dates', '')),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                bigquery.ScalarQueryParameter("date_flexibility", "STRING", application_data.get('date_flexibility', '')),
                bigquery.ScalarQueryParameter("flexibility_explanation", "STRING", application_data.get('flexibility_explanation', '')),
                bigquery.ScalarQueryParameter("sabbatical_purpose", "STRING", application_data.get('sabbatical_purpose', '')),
                bigquery.ScalarQueryParameter("why_now", "STRING", application_data.get('why_now', '')),
                bigquery.ScalarQueryParameter("coverage_plan", "STRING", application_data.get('coverage_plan', '')),
                bigquery.ScalarQueryParameter("manager_discussion", "STRING", application_data.get('manager_discussion', '')),
                bigquery.ScalarQueryParameter("ack_one_year", "BOOL", application_data.get('ack_one_year', False)),
                bigquery.ScalarQueryParameter("ack_no_other_job", "BOOL", application_data.get('ack_no_other_job', False)),
                bigquery.ScalarQueryParameter("additional_notes", "STRING", application_data.get('additional_notes', '')),
                bigquery.ScalarQueryParameter("status", "STRING", application_data.get('status', 'Submitted')),
                bigquery.ScalarQueryParameter("status_updated_at", "TIMESTAMP", status_updated_at),
                bigquery.ScalarQueryParameter("status_updated_by", "STRING", application_data.get('status_updated_by', '')),
                bigquery.ScalarQueryParameter("admin_notes", "STRING", application_data.get('admin_notes', '')),
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

        for field, value in updates.items():
            param_name = f"param_{field}"

            if field == 'status_updated_at':
                set_clauses.append(f"{field} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "TIMESTAMP", datetime.fromisoformat(value)))
            elif field in ['ack_one_year', 'ack_no_other_job']:
                set_clauses.append(f"{field} = @{param_name}")
                params.append(bigquery.ScalarQueryParameter(param_name, "BOOL", bool(value)))
            else:
                set_clauses.append(f"{field} = @{param_name}")
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
    """Decorator to require admin authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = session.get('user')
        if not user:
            return jsonify({'error': 'Authentication required'}), 401
        if user.get('email', '').lower() not in [e.lower() for e in ADMIN_USERS]:
            return jsonify({'error': 'Admin access required'}), 403
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
                          'preferred_dates', 'date_flexibility', 'sabbatical_purpose',
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
    """Look up applications by email."""
    email = request.args.get('email', '').lower().strip()

    if not email:
        return jsonify({'error': 'Email required'}), 400

    all_applications = read_all_applications()

    # Filter to applications by this email
    user_applications = [
        a for a in all_applications
        if a.get('employee_email', '').lower() == email
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
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        results = bq_client.query(query, job_config=job_config).result()

        for row in results:
            # Use preferred name if available, otherwise first name
            display_name = row.Preferred_First_Name or row.First_Name
            full_name = f"{display_name} {row.Last_Name}"
            years = row.years_of_service or 0

            # Calculate eligibility (10+ years required)
            is_eligible = years >= 10

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
            if a.get('employee_email', '').lower() == email:
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
        # Only check approved or under review applications
        if app.get('status') not in ['Approved', 'Under Review', 'Submitted']:
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

    # Filter to applications with dates that are approved, under review, or submitted
    calendar_data = []
    for app in all_applications:
        if app.get('status') in ['Approved', 'Under Review', 'Submitted']:
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


# ============ Auth Routes ============

@app.route('/login')
def login():
    """Initiate Google OAuth."""
    if not google:
        return jsonify({'error': 'OAuth not configured'}), 500
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
            session['user'] = {
                'email': user_info.get('email'),
                'name': user_info.get('name'),
                'picture': user_info.get('picture')
            }

        # Redirect back to the app with admin view
        return redirect('/?admin=true')
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
        is_admin = user.get('email', '').lower() in [e.lower() for e in ADMIN_USERS]
        return jsonify({
            'authenticated': True,
            'is_admin': is_admin,
            'user': user
        })
    return jsonify({'authenticated': False, 'is_admin': False})


# ============ Admin Routes ============

@app.route('/api/admin/applications', methods=['GET'])
@require_admin
def get_all_applications():
    """Get all applications (admin only)."""
    applications = read_all_applications()
    return jsonify({'applications': applications})


@app.route('/api/admin/applications/<application_id>', methods=['PATCH'])
@require_admin
def update_application_status(application_id):
    """Update an application (admin only)."""
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


@app.route('/api/admin/stats', methods=['GET'])
@require_admin
def get_stats():
    """Get dashboard statistics (admin only)."""
    applications = read_all_applications()

    total = len(applications)
    submitted = len([a for a in applications if a.get('status') == 'Submitted'])
    under_review = len([a for a in applications if a.get('status') == 'Under Review'])
    approved = len([a for a in applications if a.get('status') == 'Approved'])
    completed = len([a for a in applications if a.get('status') == 'Completed'])
    denied = len([a for a in applications if a.get('status') == 'Denied'])
    withdrawn = len([a for a in applications if a.get('status') == 'Withdrawn'])

    return jsonify({
        'total': total,
        'submitted': submitted,
        'under_review': under_review,
        'approved': approved,
        'completed': completed,
        'denied': denied,
        'withdrawn': withdrawn
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
