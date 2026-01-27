"""
Flask backend for Sabbatical Program Application System
Provides API endpoints for BigQuery data access
With Google OAuth 2.0 authentication and role-based views
"""

from flask import Flask, jsonify, send_from_directory, redirect, url_for, session, request
from flask_cors import CORS
from google.cloud import bigquery
from authlib.integrations.flask_client import OAuth
from werkzeug.middleware.proxy_fix import ProxyFix
from functools import wraps
from datetime import datetime, date
import logging
import os
import secrets
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Fix for running behind Cloud Run proxy
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Session configuration
app.secret_key = os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(32))
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('FLASK_ENV') != 'development'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# CORS configuration
ALLOWED_ORIGINS = os.environ.get('ALLOWED_ORIGINS', '*').split(',')
CORS(app, origins=ALLOWED_ORIGINS, supports_credentials=True)

# OAuth configuration
ALLOWED_DOMAIN = 'firstlineschools.org'
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET', '')

# Dev mode - bypasses OAuth for local testing
# Cloud Run sets K_SERVICE env var, so if that exists we're in production
IS_CLOUD_RUN = os.environ.get('K_SERVICE') is not None
DEV_MODE = not IS_CLOUD_RUN and (os.environ.get('FLASK_ENV') == 'development' or not GOOGLE_CLIENT_ID)
DEV_USER_EMAIL = 'sshirey@firstlineschools.org'

logger.info(f"GOOGLE_CLIENT_ID set: {bool(GOOGLE_CLIENT_ID)}")
logger.info(f"IS_CLOUD_RUN: {IS_CLOUD_RUN}")
logger.info(f"DEV_MODE: {DEV_MODE}")

# Role configuration
TALENT_TEAM_EMAILS = [
    'sshirey@firstlineschools.org',
]

HR_TEAM_EMAILS = [
    'brichardson@firstlineschools.org',
    'sshirey@firstlineschools.org',  # For testing
]

ADMIN_EMAILS = [
    'sshirey@firstlineschools.org',
    'brichardson@firstlineschools.org',
    'spence@firstlineschools.org',
]

# CEO for final approval
CEO_EMAILS = [
    'tcole@firstlineschools.org',  # CEO
    'sshirey@firstlineschools.org',  # For testing
]

# Eligibility requirement (years of service)
ELIGIBILITY_YEARS = 10

oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={
        'scope': 'openid email profile'
    }
)

# BigQuery configuration
PROJECT_ID = 'talent-demo-482004'
DATASET_ID = 'sabbatical'
STAFF_DATASET = 'talent_grow_observations'

# Initialize BigQuery client
try:
    client = bigquery.Client(project=PROJECT_ID)
    logger.info(f"BigQuery client initialized for project: {PROJECT_ID}")
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    client = None


# ============================================
# Authentication Helpers
# ============================================

def login_required(f):
    """Decorator to protect routes - requires valid session"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if DEV_MODE:
            if 'user' not in session:
                session['user'] = {
                    'email': DEV_USER_EMAIL,
                    'name': 'Dev User'
                }
        if 'user' not in session:
            return jsonify({'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function


def get_user_role(email):
    """Determine user's role based on email"""
    email_lower = email.lower()
    roles = ['staff']  # Everyone has staff role

    if email_lower in [e.lower() for e in TALENT_TEAM_EMAILS]:
        roles.append('talent')
    if email_lower in [e.lower() for e in HR_TEAM_EMAILS]:
        roles.append('hr')
    if email_lower in [e.lower() for e in CEO_EMAILS]:
        roles.append('ceo')
    if email_lower in [e.lower() for e in ADMIN_EMAILS]:
        roles.append('admin')

    # Check if user is a director (has direct reports)
    if is_director(email):
        roles.append('director')

    return roles


def is_director(email):
    """Check if user has direct reports in staff list"""
    if not client:
        return False
    try:
        # Get the user's name first, then check if anyone reports to them
        name_query = f"""
            SELECT CONCAT(First_Name, ' ', Last_Name) as full_name
            FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
            WHERE LOWER(Email_Address) = LOWER(@email)
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        name_results = list(client.query(name_query, job_config=job_config).result())
        if not name_results:
            return False

        user_name = name_results[0].full_name

        # Check if anyone has this person as their supervisor
        query = f"""
            SELECT COUNT(*) as report_count
            FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
            WHERE Supervisor_Name__Unsecured_ = @name
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", user_name)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())
        return results[0].report_count > 0 if results else False
    except Exception as e:
        logger.error(f"Error checking director status: {e}")
        return False


# ============================================
# OAuth Routes
# ============================================

@app.route('/auth/login')
def login():
    """Initiate Google OAuth login"""
    if DEV_MODE:
        session['user'] = {'email': DEV_USER_EMAIL, 'name': 'Dev User'}
        return redirect('/')

    redirect_uri = url_for('authorize', _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route('/auth/callback')
def authorize():
    """Handle OAuth callback"""
    if DEV_MODE:
        return redirect('/')

    try:
        token = google.authorize_access_token()
        user_info = token.get('userinfo')

        if not user_info:
            return jsonify({'error': 'Failed to get user info'}), 400

        email = user_info.get('email', '')
        if not email.endswith(f'@{ALLOWED_DOMAIN}'):
            return jsonify({'error': f'Only {ALLOWED_DOMAIN} accounts allowed'}), 403

        session['user'] = {
            'email': email,
            'name': user_info.get('name', email.split('@')[0])
        }

        return redirect('/')
    except Exception as e:
        logger.error(f"OAuth error: {e}")
        return jsonify({'error': 'Authentication failed'}), 500


@app.route('/auth/logout')
def logout():
    """Clear session and logout"""
    session.clear()
    return redirect('/')


@app.route('/api/user')
@login_required
def get_user():
    """Get current user info and roles"""
    user = session.get('user', {})
    email = user.get('email', '')
    roles = get_user_role(email)

    return jsonify({
        'email': email,
        'name': user.get('name', ''),
        'roles': roles
    })


# ============================================
# Employee Lookup APIs
# ============================================

@app.route('/api/employee-lookup')
@login_required
def employee_lookup():
    """Look up employee data by email"""
    email = request.args.get('email', session.get('user', {}).get('email', ''))

    if not client:
        return jsonify({'error': 'Database unavailable'}), 503

    try:
        query = f"""
            SELECT
                Employee_Number as name_key,
                First_Name,
                Last_Name,
                CONCAT(First_Name, ' ', Last_Name) as full_name,
                Email_Address,
                Last_Hire_Date,
                DATE_DIFF(CURRENT_DATE(), DATE(Last_Hire_Date), DAY) / 365.25 as years_of_service,
                Job_Title,
                Function as department,
                Location_Name as site,
                Supervisor_Name__Unsecured_
            FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
            WHERE LOWER(Email_Address) = LOWER(@email)
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())

        if not results:
            return jsonify({'error': 'Employee not found'}), 404

        row = results[0]
        return jsonify({
            'name_key': row.name_key,
            'first_name': row.First_Name,
            'last_name': row.Last_Name,
            'full_name': row.full_name,
            'email': row.Email_Address,
            'hire_date': row.Last_Hire_Date.isoformat() if row.Last_Hire_Date else None,
            'years_of_service': round(row.years_of_service, 1) if row.years_of_service else 0,
            'job_title': row.Job_Title,
            'department': row.department,
            'site': row.site,
            'supervisor_name': row.Supervisor_Name__Unsecured_
        })
    except Exception as e:
        logger.error(f"Employee lookup error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/eligibility-check')
@login_required
def eligibility_check():
    """Check if employee is eligible for sabbatical (10+ years)"""
    email = request.args.get('email', session.get('user', {}).get('email', ''))

    if not client:
        return jsonify({'error': 'Database unavailable'}), 503

    try:
        query = f"""
            SELECT
                CONCAT(First_Name, ' ', Last_Name) as full_name,
                Last_Hire_Date,
                DATE_DIFF(CURRENT_DATE(), DATE(Last_Hire_Date), DAY) / 365.25 as years_of_service
            FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
            WHERE LOWER(Email_Address) = LOWER(@email)
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())

        if not results:
            return jsonify({'error': 'Employee not found'}), 404

        row = results[0]
        years = row.years_of_service or 0
        eligible = years >= ELIGIBILITY_YEARS

        return jsonify({
            'eligible': eligible,
            'years_of_service': round(years, 1),
            'years_required': ELIGIBILITY_YEARS,
            'years_until_eligible': max(0, round(ELIGIBILITY_YEARS - years, 1)),
            'hire_date': row.Last_Hire_Date.isoformat() if row.Last_Hire_Date else None
        })
    except Exception as e:
        logger.error(f"Eligibility check error: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================
# Application APIs
# ============================================

@app.route('/api/applications', methods=['GET'])
@login_required
def get_applications():
    """Get applications based on user's role"""
    user = session.get('user', {})
    email = user.get('email', '')
    roles = get_user_role(email)
    status_filter = request.args.get('status', '')

    if not client:
        return jsonify({'error': 'Database unavailable'}), 503

    try:
        # Build query based on role
        if 'admin' in roles or 'hr' in roles:
            # HR/Admin sees all applications
            where_clause = "WHERE 1=1"
        elif 'talent' in roles:
            # Talent sees pending_talent and can view history
            where_clause = "WHERE 1=1"
        elif 'director' in roles:
            # Directors see their direct reports' applications
            # First get the director's name, then find their direct reports
            where_clause = f"""
                WHERE employee_email IN (
                    SELECT Email_Address FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
                    WHERE Supervisor_Name__Unsecured_ = (
                        SELECT CONCAT(First_Name, ' ', Last_Name)
                        FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
                        WHERE LOWER(Email_Address) = LOWER('{email}')
                        LIMIT 1
                    )
                )
            """
        else:
            # Staff sees only their own
            where_clause = f"WHERE LOWER(employee_email) = LOWER('{email}')"

        if status_filter:
            where_clause += f" AND status = '{status_filter}'"

        query = f"""
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.applications`
            {where_clause}
            ORDER BY submitted_at DESC
        """

        results = list(client.query(query).result())

        applications = []
        for row in results:
            applications.append({
                'application_id': row.application_id,
                'employee_name': row.employee_name,
                'employee_email': row.employee_email,
                'years_of_service': row.years_of_service,
                'job_title': row.job_title,
                'department': row.department,
                'site': row.site,
                'requested_start_date': row.requested_start_date.isoformat() if row.requested_start_date else None,
                'requested_end_date': row.requested_end_date.isoformat() if row.requested_end_date else None,
                'duration_weeks': row.duration_weeks,
                'sabbatical_purpose': row.sabbatical_purpose,
                'status': row.status,
                'submitted_at': row.submitted_at.isoformat() if row.submitted_at else None,
                'talent_reviewer': row.talent_reviewer,
                'talent_decision': row.talent_decision,
                'talent_notes': row.talent_notes,
                'talent_reviewed_at': row.talent_reviewed_at.isoformat() if row.talent_reviewed_at else None,
                'hr_reviewer': row.hr_reviewer,
                'hr_decision': row.hr_decision,
                'hr_notes': row.hr_notes,
                'hr_reviewed_at': row.hr_reviewed_at.isoformat() if row.hr_reviewed_at else None
            })

        return jsonify({'applications': applications})
    except Exception as e:
        logger.error(f"Get applications error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/applications', methods=['POST'])
@login_required
def submit_application():
    """Submit a new sabbatical application"""
    user = session.get('user', {})
    email = user.get('email', '')
    data = request.get_json()

    if not client:
        return jsonify({'error': 'Database unavailable'}), 503

    try:
        # Check eligibility first
        eligibility = eligibility_check_internal(email)
        if not eligibility['eligible']:
            return jsonify({
                'error': f"Not eligible for sabbatical. {eligibility['years_of_service']} years of service, {ELIGIBILITY_YEARS} required."
            }), 400

        # Check for duplicate pending applications
        duplicate = check_duplicate_internal(email)
        if duplicate:
            return jsonify({
                'error': 'You already have a pending or approved sabbatical application.'
            }), 400

        # Get employee data
        emp_data = get_employee_data(email)
        if not emp_data:
            return jsonify({'error': 'Employee data not found'}), 404

        # Calculate duration
        start_date = datetime.strptime(data['start_date'], '%Y-%m-%d').date()
        end_date = datetime.strptime(data['end_date'], '%Y-%m-%d').date()
        duration_weeks = (end_date - start_date).days // 7

        application_id = str(uuid.uuid4())
        now = datetime.utcnow()

        # Get supervisor info for director routing
        supervisor_name = emp_data.get('supervisor_name', '')

        # Insert application with all fields - initial status is pending_director
        query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.applications`
            (application_id, employee_name_key, employee_name, employee_email, hire_date,
             years_of_service, job_title, department, site, requested_start_date,
             requested_end_date, duration_weeks, leave_weeks, salary_percentage,
             sabbatical_purpose, why_now, coverage_plan, flexible, flexibility_details,
             manager_discussed, additional_comments, supervisor_name, status,
             submitted_at, created_at, updated_at)
            VALUES
            (@app_id, @name_key, @name, @email, @hire_date,
             @years, @job_title, @dept, @site, @start_date,
             @end_date, @duration, @leave_weeks, @salary_pct,
             @purpose, @why_now, @coverage, @flexible, @flex_details,
             @manager_discussed, @comments, @supervisor, 'pending_director',
             @now, @now, @now)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("app_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("name_key", "STRING", emp_data['name_key']),
                bigquery.ScalarQueryParameter("name", "STRING", emp_data['full_name']),
                bigquery.ScalarQueryParameter("email", "STRING", email),
                bigquery.ScalarQueryParameter("hire_date", "DATE", emp_data['hire_date']),
                bigquery.ScalarQueryParameter("years", "FLOAT64", emp_data['years_of_service']),
                bigquery.ScalarQueryParameter("job_title", "STRING", emp_data['job_title']),
                bigquery.ScalarQueryParameter("dept", "STRING", emp_data['department']),
                bigquery.ScalarQueryParameter("site", "STRING", emp_data['site']),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                bigquery.ScalarQueryParameter("duration", "INT64", duration_weeks),
                bigquery.ScalarQueryParameter("leave_weeks", "INT64", data.get('leave_weeks', duration_weeks)),
                bigquery.ScalarQueryParameter("salary_pct", "INT64", data.get('salary_percentage', 100)),
                bigquery.ScalarQueryParameter("purpose", "STRING", data.get('purpose', '')),
                bigquery.ScalarQueryParameter("why_now", "STRING", data.get('why_now', '')),
                bigquery.ScalarQueryParameter("coverage", "STRING", data.get('coverage_plan', '')),
                bigquery.ScalarQueryParameter("flexible", "BOOL", data.get('flexible', False)),
                bigquery.ScalarQueryParameter("flex_details", "STRING", data.get('flexibility_details', '')),
                bigquery.ScalarQueryParameter("manager_discussed", "BOOL", data.get('manager_discussed', False)),
                bigquery.ScalarQueryParameter("comments", "STRING", data.get('additional_comments', '')),
                bigquery.ScalarQueryParameter("supervisor", "STRING", supervisor_name),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
            ]
        )

        client.query(query, job_config=job_config).result()

        # Log to history
        log_history(application_id, 'submitted', email, emp_data['full_name'], 'Application submitted')

        # Send notification to director (via employee's supervisor)
        app_data = {
            'application_id': application_id,
            'employee_name': emp_data['full_name'],
            'employee_email': email,
            'job_title': emp_data['job_title'],
            'department': emp_data['department'],
            'site': emp_data['site'],
            'years_of_service': emp_data['years_of_service'],
            'requested_start_date': data['start_date'],
            'requested_end_date': data['end_date'],
            'duration_weeks': duration_weeks,
            'sabbatical_purpose': data.get('purpose', '')
        }
        send_notification('submitted_to_talent', app_data)  # Will update template name later
        send_notification('submitted_confirmation', app_data)

        return jsonify({
            'success': True,
            'application_id': application_id,
            'message': 'Application submitted successfully'
        })
    except Exception as e:
        logger.error(f"Submit application error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/applications/<application_id>/review', methods=['POST'])
@login_required
def review_application(application_id):
    """Review an application (Director → Talent → CEO workflow)"""
    user = session.get('user', {})
    email = user.get('email', '')
    roles = get_user_role(email)
    data = request.get_json()

    decision = data.get('decision')  # 'approved' or 'denied'
    notes = data.get('notes', '')

    if decision not in ['approved', 'denied']:
        return jsonify({'error': 'Invalid decision'}), 400

    if not client:
        return jsonify({'error': 'Database unavailable'}), 503

    try:
        # Get current application status and details
        query = f"""
            SELECT status, employee_name, employee_email, supervisor_name,
                   job_title, department, site, years_of_service,
                   requested_start_date, requested_end_date, duration_weeks,
                   sabbatical_purpose, talent_notes
            FROM `{PROJECT_ID}.{DATASET_ID}.applications`
            WHERE application_id = @app_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("app_id", "STRING", application_id)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())

        if not results:
            return jsonify({'error': 'Application not found'}), 404

        row = results[0]
        current_status = row.status
        employee_name = row.employee_name
        now = datetime.utcnow()

        # Build app_data for email notifications
        app_data = {
            'application_id': application_id,
            'employee_name': employee_name,
            'employee_email': row.employee_email,
            'job_title': row.job_title,
            'department': row.department,
            'site': row.site,
            'years_of_service': row.years_of_service,
            'requested_start_date': row.requested_start_date.isoformat() if row.requested_start_date else '',
            'requested_end_date': row.requested_end_date.isoformat() if row.requested_end_date else '',
            'duration_weeks': row.duration_weeks,
            'sabbatical_purpose': row.sabbatical_purpose,
            'talent_notes': row.talent_notes or notes
        }

        # Determine which review this is based on status and user role
        # Workflow: pending_director → pending_talent → pending_ceo → approved

        if current_status == 'pending_director' and ('director' in roles or 'admin' in roles):
            # Director review (first stage)
            if decision == 'approved':
                new_status = 'pending_talent'
                action = 'director_approved'
            else:
                new_status = 'denied'
                action = 'director_denied'

            update_query = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.applications`
                SET status = @status,
                    director_reviewer = @reviewer,
                    director_decision = @decision,
                    director_notes = @notes,
                    director_reviewed_at = @now,
                    updated_at = @now
                WHERE application_id = @app_id
            """

        elif current_status == 'pending_talent' and ('talent' in roles or 'hr' in roles or 'admin' in roles):
            # Talent/HR review (second stage)
            if decision == 'approved':
                new_status = 'pending_ceo'
                action = 'talent_approved'
            else:
                new_status = 'denied'
                action = 'talent_denied'

            update_query = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.applications`
                SET status = @status,
                    talent_reviewer = @reviewer,
                    talent_decision = @decision,
                    talent_notes = @notes,
                    talent_reviewed_at = @now,
                    updated_at = @now
                WHERE application_id = @app_id
            """

        elif current_status == 'pending_ceo' and ('ceo' in roles or 'admin' in roles):
            # CEO review (final stage)
            if decision == 'approved':
                new_status = 'approved'
                action = 'ceo_approved'
            else:
                new_status = 'denied'
                action = 'ceo_denied'

            update_query = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.applications`
                SET status = @status,
                    ceo_reviewer = @reviewer,
                    ceo_decision = @decision,
                    ceo_notes = @notes,
                    ceo_reviewed_at = @now,
                    updated_at = @now
                WHERE application_id = @app_id
            """

        else:
            return jsonify({'error': 'You cannot review this application at this stage'}), 403

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", new_status),
                bigquery.ScalarQueryParameter("reviewer", "STRING", email),
                bigquery.ScalarQueryParameter("decision", "STRING", decision),
                bigquery.ScalarQueryParameter("notes", "STRING", notes),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("app_id", "STRING", application_id),
            ]
        )

        client.query(update_query, job_config=job_config).result()

        # Log to history
        log_history(application_id, action, email, user.get('name', email), notes)

        # Send appropriate notifications
        app_data['talent_notes'] = notes  # Use current notes for notification
        app_data['hr_notes'] = notes

        if action == 'talent_approved':
            send_notification('talent_approved', app_data)
            # Could add notification to CEO here
        elif action == 'talent_denied':
            send_notification('talent_denied', app_data)
        elif action == 'ceo_approved':
            send_notification('hr_approved', app_data)  # Reuse the congratulations template
        elif action == 'ceo_denied':
            send_notification('hr_denied', app_data)  # Reuse the denial template

        return jsonify({
            'success': True,
            'new_status': new_status,
            'message': f'Application {decision}'
        })
    except Exception as e:
        logger.error(f"Review application error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/applications/<application_id>/withdraw', methods=['POST'])
@login_required
def withdraw_application(application_id):
    """Withdraw an application (staff only, before final approval)"""
    user = session.get('user', {})
    email = user.get('email', '')

    if not client:
        return jsonify({'error': 'Database unavailable'}), 503

    try:
        # Verify ownership and status
        query = f"""
            SELECT status, employee_email
            FROM `{PROJECT_ID}.{DATASET_ID}.applications`
            WHERE application_id = @app_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("app_id", "STRING", application_id)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())

        if not results:
            return jsonify({'error': 'Application not found'}), 404

        app_email = results[0].employee_email
        status = results[0].status

        if app_email.lower() != email.lower():
            return jsonify({'error': 'You can only withdraw your own application'}), 403

        if status not in ['pending_talent', 'pending_hr']:
            return jsonify({'error': 'Cannot withdraw application at this stage'}), 400

        now = datetime.utcnow()
        update_query = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.applications`
            SET status = 'withdrawn', updated_at = @now
            WHERE application_id = @app_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("app_id", "STRING", application_id),
            ]
        )

        client.query(update_query, job_config=job_config).result()

        log_history(application_id, 'withdrawn', email, user.get('name', email), 'Application withdrawn by employee')

        return jsonify({'success': True, 'message': 'Application withdrawn'})
    except Exception as e:
        logger.error(f"Withdraw application error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/site-conflicts')
@login_required
def check_site_conflicts():
    """Check for sabbatical conflicts at the same site"""
    site = request.args.get('site', '')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    exclude_id = request.args.get('exclude_id', '')

    if not all([site, start_date, end_date]):
        return jsonify({'error': 'Missing required parameters'}), 400

    if not client:
        return jsonify({'error': 'Database unavailable'}), 503

    try:
        query = f"""
            SELECT employee_name, requested_start_date, requested_end_date
            FROM `{PROJECT_ID}.{DATASET_ID}.applications`
            WHERE site = @site
              AND status IN ('approved', 'pending_hr')
              AND application_id != @exclude_id
              AND (
                  (requested_start_date <= @end_date AND requested_end_date >= @start_date)
              )
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site", "STRING", site),
                bigquery.ScalarQueryParameter("start_date", "DATE", datetime.strptime(start_date, '%Y-%m-%d').date()),
                bigquery.ScalarQueryParameter("end_date", "DATE", datetime.strptime(end_date, '%Y-%m-%d').date()),
                bigquery.ScalarQueryParameter("exclude_id", "STRING", exclude_id or ''),
            ]
        )

        results = list(client.query(query, job_config=job_config).result())

        conflicts = []
        for row in results:
            conflicts.append({
                'employee_name': row.employee_name,
                'start_date': row.requested_start_date.isoformat(),
                'end_date': row.requested_end_date.isoformat()
            })

        return jsonify({
            'has_conflicts': len(conflicts) > 0,
            'conflicts': conflicts
        })
    except Exception as e:
        logger.error(f"Site conflicts check error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/duplicate-check')
@login_required
def duplicate_check():
    """Check for existing applications by employee"""
    email = request.args.get('email', session.get('user', {}).get('email', ''))

    result = check_duplicate_internal(email)
    return jsonify({'has_duplicate': result is not None, 'existing_application': result})


# ============================================
# Helper Functions
# ============================================

def eligibility_check_internal(email):
    """Internal eligibility check"""
    try:
        query = f"""
            SELECT
                DATE_DIFF(CURRENT_DATE(), Hire_Date, DAY) / 365.25 as years_of_service
            FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
            WHERE LOWER(Email) = LOWER(@email)
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())

        if not results:
            return {'eligible': False, 'years_of_service': 0}

        years = results[0].years_of_service or 0
        return {
            'eligible': years >= ELIGIBILITY_YEARS,
            'years_of_service': round(years, 1)
        }
    except Exception as e:
        logger.error(f"Eligibility check internal error: {e}")
        return {'eligible': False, 'years_of_service': 0}


def check_duplicate_internal(email):
    """Check for existing pending/approved applications"""
    try:
        query = f"""
            SELECT application_id, status, submitted_at
            FROM `{PROJECT_ID}.{DATASET_ID}.applications`
            WHERE LOWER(employee_email) = LOWER(@email)
              AND status IN ('pending_talent', 'pending_hr', 'approved')
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())

        if results:
            row = results[0]
            return {
                'application_id': row.application_id,
                'status': row.status,
                'submitted_at': row.submitted_at.isoformat() if row.submitted_at else None
            }
        return None
    except Exception as e:
        logger.error(f"Duplicate check error: {e}")
        return None


def get_employee_data(email):
    """Get employee data for application"""
    try:
        query = f"""
            SELECT
                Employee_Number as name_key,
                CONCAT(First_Name, ' ', Last_Name) as full_name,
                DATE(Last_Hire_Date) as hire_date,
                DATE_DIFF(CURRENT_DATE(), DATE(Last_Hire_Date), DAY) / 365.25 as years_of_service,
                Job_Title as job_title,
                Function as department,
                Location_Name as site
            FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
            WHERE LOWER(Email_Address) = LOWER(@email)
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())

        if results:
            row = results[0]
            return {
                'name_key': row.name_key,
                'full_name': row.full_name,
                'hire_date': row.hire_date,
                'years_of_service': round(row.years_of_service, 1) if row.years_of_service else 0,
                'job_title': row.job_title,
                'department': row.department,
                'site': row.site
            }
        return None
    except Exception as e:
        logger.error(f"Get employee data error: {e}")
        return None


def log_history(application_id, action, actor_email, actor_name, notes=''):
    """Log action to approval history"""
    try:
        history_id = str(uuid.uuid4())
        now = datetime.utcnow()

        query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.approval_history`
            (history_id, application_id, action, actor_email, actor_name, notes, created_at)
            VALUES
            (@history_id, @app_id, @action, @email, @name, @notes, @now)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("history_id", "STRING", history_id),
                bigquery.ScalarQueryParameter("app_id", "STRING", application_id),
                bigquery.ScalarQueryParameter("action", "STRING", action),
                bigquery.ScalarQueryParameter("email", "STRING", actor_email),
                bigquery.ScalarQueryParameter("name", "STRING", actor_name),
                bigquery.ScalarQueryParameter("notes", "STRING", notes),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
            ]
        )
        client.query(query, job_config=job_config).result()
    except Exception as e:
        logger.error(f"Log history error: {e}")


# ============================================
# Email Notification System
# ============================================

# Email addresses for notifications
TALENT_TEAM_EMAIL = 'talent@firstlineschools.org'
HR_TEAM_EMAIL = 'hr@firstlineschools.org'

def get_email_template(template_type, app_data):
    """Generate email content based on template type and application data"""

    templates = {
        'submitted_to_talent': {
            'to': TALENT_TEAM_EMAIL,
            'subject': f"New Sabbatical Application - {app_data.get('employee_name', 'Employee')}",
            'body': f"""
Hello Talent Team,

A new sabbatical application has been submitted and requires your review.

APPLICANT DETAILS
-----------------
Name: {app_data.get('employee_name', 'N/A')}
Email: {app_data.get('employee_email', 'N/A')}
Job Title: {app_data.get('job_title', 'N/A')}
Department: {app_data.get('department', 'N/A')}
Site: {app_data.get('site', 'N/A')}
Years of Service: {app_data.get('years_of_service', 'N/A')} years

REQUESTED DATES
---------------
Start Date: {app_data.get('requested_start_date', 'N/A')}
End Date: {app_data.get('requested_end_date', 'N/A')}
Duration: {app_data.get('duration_weeks', 'N/A')} weeks

SABBATICAL PURPOSE
------------------
{app_data.get('sabbatical_purpose', 'No description provided.')}

Please log in to the Sabbatical Program portal to review this application:
https://sabbatical-program-965913991496.us-central1.run.app

Thank you,
FirstLine Schools Sabbatical Program
"""
        },

        'submitted_confirmation': {
            'to': app_data.get('employee_email', ''),
            'subject': "Your Sabbatical Application Has Been Submitted",
            'body': f"""
Dear {app_data.get('employee_name', 'Team Member')},

Thank you for submitting your sabbatical application! We're excited that you're taking advantage of this benefit after {app_data.get('years_of_service', 'many')} years of dedicated service to FirstLine Schools.

APPLICATION SUMMARY
-------------------
Requested Dates: {app_data.get('requested_start_date', 'N/A')} to {app_data.get('requested_end_date', 'N/A')}
Duration: {app_data.get('duration_weeks', 'N/A')} weeks

WHAT HAPPENS NEXT
-----------------
1. The Talent team will review your application
2. If approved, it will move to HR for final approval
3. You'll receive email updates at each stage

You can track your application status at:
https://sabbatical-program-965913991496.us-central1.run.app

If you have questions, please contact the Talent team.

Congratulations on reaching this milestone!

Best regards,
FirstLine Schools Sabbatical Program
"""
        },

        'talent_approved': {
            'to': app_data.get('employee_email', ''),
            'subject': "Sabbatical Application Update - Talent Review Complete",
            'body': f"""
Dear {app_data.get('employee_name', 'Team Member')},

Great news! Your sabbatical application has been reviewed and APPROVED by the Talent team.

APPLICATION STATUS: Approved by Talent - Pending HR Review
----------------------------------------------------------
Requested Dates: {app_data.get('requested_start_date', 'N/A')} to {app_data.get('requested_end_date', 'N/A')}

Reviewer Notes: {app_data.get('talent_notes', 'No additional notes.')}

NEXT STEPS
----------
Your application has been forwarded to HR for final approval. You will receive another notification once HR completes their review.

Track your application at:
https://sabbatical-program-965913991496.us-central1.run.app

Best regards,
FirstLine Schools Sabbatical Program
"""
        },

        'talent_approved_to_hr': {
            'to': HR_TEAM_EMAIL,
            'subject': f"Sabbatical Application Ready for HR Review - {app_data.get('employee_name', 'Employee')}",
            'body': f"""
Hello HR Team,

A sabbatical application has been approved by Talent and is ready for your final review.

APPLICANT DETAILS
-----------------
Name: {app_data.get('employee_name', 'N/A')}
Email: {app_data.get('employee_email', 'N/A')}
Job Title: {app_data.get('job_title', 'N/A')}
Department: {app_data.get('department', 'N/A')}
Site: {app_data.get('site', 'N/A')}
Years of Service: {app_data.get('years_of_service', 'N/A')} years

REQUESTED DATES
---------------
Start Date: {app_data.get('requested_start_date', 'N/A')}
End Date: {app_data.get('requested_end_date', 'N/A')}
Duration: {app_data.get('duration_weeks', 'N/A')} weeks

TALENT REVIEW
-------------
Reviewed by: {app_data.get('talent_reviewer', 'N/A')}
Decision: APPROVED
Notes: {app_data.get('talent_notes', 'No notes.')}

Please log in to complete the final review:
https://sabbatical-program-965913991496.us-central1.run.app

Thank you,
FirstLine Schools Sabbatical Program
"""
        },

        'talent_denied': {
            'to': app_data.get('employee_email', ''),
            'subject': "Sabbatical Application Update - Review Decision",
            'body': f"""
Dear {app_data.get('employee_name', 'Team Member')},

Thank you for your interest in the sabbatical program. After careful review, we regret to inform you that your application has not been approved at this time.

APPLICATION STATUS: Not Approved
--------------------------------
Requested Dates: {app_data.get('requested_start_date', 'N/A')} to {app_data.get('requested_end_date', 'N/A')}

Reviewer Feedback: {app_data.get('talent_notes', 'Please contact the Talent team for more information.')}

NEXT STEPS
----------
If you have questions about this decision or would like to discuss alternative dates, please reach out to the Talent team at talent@firstlineschools.org.

You may submit a new application with different dates if appropriate.

Best regards,
FirstLine Schools Sabbatical Program
"""
        },

        'hr_approved': {
            'to': app_data.get('employee_email', ''),
            'subject': "CONGRATULATIONS! Your Sabbatical Has Been Approved!",
            'body': f"""
Dear {app_data.get('employee_name', 'Team Member')},

CONGRATULATIONS! Your sabbatical application has been FULLY APPROVED!

After {app_data.get('years_of_service', 'many')} years of dedicated service to FirstLine Schools, you have earned this well-deserved break. We celebrate your commitment and look forward to your return.

APPROVED SABBATICAL DETAILS
---------------------------
Start Date: {app_data.get('requested_start_date', 'N/A')}
End Date: {app_data.get('requested_end_date', 'N/A')}
Duration: {app_data.get('duration_weeks', 'N/A')} weeks

HR Notes: {app_data.get('hr_notes', 'Congratulations on this milestone!')}

NEXT STEPS
----------
1. HR will contact you to discuss logistics and coverage planning
2. Work with your supervisor to prepare for your absence
3. Enjoy your well-earned sabbatical!

Thank you for your years of service to FirstLine Schools and our students.

Best regards,
FirstLine Schools Sabbatical Program

P.S. We'd love to hear about your sabbatical plans and experiences when you return!
"""
        },

        'hr_denied': {
            'to': app_data.get('employee_email', ''),
            'subject': "Sabbatical Application Update - HR Review Decision",
            'body': f"""
Dear {app_data.get('employee_name', 'Team Member')},

Thank you for your sabbatical application. After review by both the Talent team and HR, we regret to inform you that we are unable to approve your request at this time.

APPLICATION STATUS: Not Approved
--------------------------------
Requested Dates: {app_data.get('requested_start_date', 'N/A')} to {app_data.get('requested_end_date', 'N/A')}

HR Feedback: {app_data.get('hr_notes', 'Please contact HR for more information.')}

NEXT STEPS
----------
If you have questions about this decision or would like to discuss alternatives, please reach out to HR at hr@firstlineschools.org.

We value your service to FirstLine Schools and encourage you to apply again when circumstances permit.

Best regards,
FirstLine Schools Sabbatical Program
"""
        }
    }

    return templates.get(template_type, None)


def send_notification(template_type, app_data):
    """Send email notification and log to BigQuery"""
    template = get_email_template(template_type, app_data)
    if not template:
        logger.error(f"Unknown email template: {template_type}")
        return False

    try:
        # Log to BigQuery notifications_log
        notification_id = str(uuid.uuid4())
        now = datetime.utcnow()

        query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.notifications_log`
            (notification_id, application_id, template_type, recipient_email, subject, sent_at)
            VALUES
            (@notif_id, @app_id, @template, @recipient, @subject, @now)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("notif_id", "STRING", notification_id),
                bigquery.ScalarQueryParameter("app_id", "STRING", app_data.get('application_id', '')),
                bigquery.ScalarQueryParameter("template", "STRING", template_type),
                bigquery.ScalarQueryParameter("recipient", "STRING", template['to']),
                bigquery.ScalarQueryParameter("subject", "STRING", template['subject']),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
            ]
        )
        client.query(query, job_config=job_config).result()

        # TODO: Integrate with actual email service (SendGrid, Mailgun, etc.)
        # For now, just log the email
        logger.info(f"Email notification logged: {template_type} to {template['to']}")
        logger.info(f"Subject: {template['subject']}")

        return True
    except Exception as e:
        logger.error(f"Send notification error: {e}")
        return False


@app.route('/api/email-preview/<template_type>')
@login_required
def preview_email(template_type):
    """Preview email templates (for demo/testing)"""
    roles = get_user_role(session.get('user', {}).get('email', ''))

    if 'admin' not in roles:
        return jsonify({'error': 'Admin access required'}), 403

    # Sample data for preview
    sample_data = {
        'application_id': 'preview-123',
        'employee_name': 'Maria Rodriguez',
        'employee_email': 'mrodriguez@firstlineschools.org',
        'job_title': 'Lead Teacher',
        'department': 'Academics',
        'site': 'Samuel J. Green Charter School',
        'years_of_service': '15.3',
        'requested_start_date': 'June 1, 2026',
        'requested_end_date': 'August 15, 2026',
        'duration_weeks': '11',
        'sabbatical_purpose': 'I plan to travel to Spain to immerse myself in the language and culture, which will enhance my Spanish instruction when I return. I also want to spend quality time with my aging parents.',
        'talent_reviewer': 'sshirey@firstlineschools.org',
        'talent_notes': 'Excellent application. Strong candidate with great plans.',
        'hr_notes': 'Coverage plan approved. Have a wonderful sabbatical!'
    }

    template = get_email_template(template_type, sample_data)

    if not template:
        return jsonify({'error': f'Unknown template: {template_type}'}), 404

    return jsonify({
        'template_type': template_type,
        'to': template['to'],
        'subject': template['subject'],
        'body': template['body']
    })


@app.route('/api/email-templates')
@login_required
def list_email_templates():
    """List all available email templates"""
    roles = get_user_role(session.get('user', {}).get('email', ''))

    if 'admin' not in roles:
        return jsonify({'error': 'Admin access required'}), 403

    templates = [
        {'id': 'submitted_to_talent', 'name': 'New Application → Talent Team', 'description': 'Sent to Talent when employee submits application'},
        {'id': 'submitted_confirmation', 'name': 'Submission Confirmation → Employee', 'description': 'Confirmation sent to employee after submitting'},
        {'id': 'talent_approved', 'name': 'Talent Approved → Employee', 'description': 'Sent to employee when Talent approves'},
        {'id': 'talent_approved_to_hr', 'name': 'Ready for HR → HR Team', 'description': 'Sent to HR when Talent approves'},
        {'id': 'talent_denied', 'name': 'Talent Denied → Employee', 'description': 'Sent to employee when Talent denies'},
        {'id': 'hr_approved', 'name': 'HR Approved (Final) → Employee', 'description': 'Congratulations email when fully approved'},
        {'id': 'hr_denied', 'name': 'HR Denied → Employee', 'description': 'Sent to employee when HR denies'}
    ]

    return jsonify({'templates': templates})


@app.route('/api/application-history/<application_id>')
@login_required
def get_application_history(application_id):
    """Get approval history for an application"""
    if not client:
        return jsonify({'error': 'Database unavailable'}), 503

    try:
        query = f"""
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.approval_history`
            WHERE application_id = @app_id
            ORDER BY created_at ASC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("app_id", "STRING", application_id)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())

        history = []
        for row in results:
            history.append({
                'action': row.action,
                'actor_name': row.actor_name,
                'notes': row.notes,
                'created_at': row.created_at.isoformat() if row.created_at else None
            })

        return jsonify({'history': history})
    except Exception as e:
        logger.error(f"Get history error: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================
# Static File Routes
# ============================================

@app.route('/')
def index():
    """Serve main application page"""
    return send_from_directory('.', 'index.html')


@app.route('/<path:filename>')
def static_files(filename):
    """Serve static files"""
    return send_from_directory('.', filename)


# ============================================
# Main Entry Point
# ============================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    debug = os.environ.get('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=port, debug=debug)
