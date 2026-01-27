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
DEV_MODE = os.environ.get('FLASK_ENV') == 'development' or not GOOGLE_CLIENT_ID
DEV_USER_EMAIL = 'sshirey@firstlineschools.org'

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
        query = f"""
            SELECT COUNT(*) as report_count
            FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
            WHERE LOWER(Supervisor_Email) = LOWER(@email)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
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
                name_key,
                First_Name,
                Last_Name,
                CONCAT(First_Name, ' ', Last_Name) as full_name,
                Email,
                Hire_Date,
                DATE_DIFF(CURRENT_DATE(), Hire_Date, DAY) / 365.25 as years_of_service,
                Job_Title,
                Function as department,
                Site,
                Supervisor_Email
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
            return jsonify({'error': 'Employee not found'}), 404

        row = results[0]
        return jsonify({
            'name_key': row.name_key,
            'first_name': row.First_Name,
            'last_name': row.Last_Name,
            'full_name': row.full_name,
            'email': row.Email,
            'hire_date': row.Hire_Date.isoformat() if row.Hire_Date else None,
            'years_of_service': round(row.years_of_service, 1) if row.years_of_service else 0,
            'job_title': row.Job_Title,
            'department': row.department,
            'site': row.Site,
            'supervisor_email': row.Supervisor_Email
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
                Hire_Date,
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
            return jsonify({'error': 'Employee not found'}), 404

        row = results[0]
        years = row.years_of_service or 0
        eligible = years >= ELIGIBILITY_YEARS

        return jsonify({
            'eligible': eligible,
            'years_of_service': round(years, 1),
            'years_required': ELIGIBILITY_YEARS,
            'years_until_eligible': max(0, round(ELIGIBILITY_YEARS - years, 1)),
            'hire_date': row.Hire_Date.isoformat() if row.Hire_Date else None
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
            # Directors see their team's applications
            where_clause = f"""
                WHERE employee_email IN (
                    SELECT Email FROM `{PROJECT_ID}.{STAFF_DATASET}.staff_master_list_with_function`
                    WHERE LOWER(Supervisor_Email) = LOWER('{email}')
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

        # Insert application
        query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.applications`
            (application_id, employee_name_key, employee_name, employee_email, hire_date,
             years_of_service, job_title, department, site, requested_start_date,
             requested_end_date, duration_weeks, sabbatical_purpose, status,
             submitted_at, created_at, updated_at)
            VALUES
            (@app_id, @name_key, @name, @email, @hire_date,
             @years, @job_title, @dept, @site, @start_date,
             @end_date, @duration, @purpose, 'pending_talent',
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
                bigquery.ScalarQueryParameter("purpose", "STRING", data.get('purpose', '')),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
            ]
        )

        client.query(query, job_config=job_config).result()

        # Log to history
        log_history(application_id, 'submitted', email, emp_data['full_name'], 'Application submitted')

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
    """Review an application (Talent or HR)"""
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
        # Get current application status
        query = f"""
            SELECT status, employee_name
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

        current_status = results[0].status
        employee_name = results[0].employee_name
        now = datetime.utcnow()

        # Determine which review this is
        if current_status == 'pending_talent' and ('talent' in roles or 'admin' in roles):
            # Talent review
            if decision == 'approved':
                new_status = 'pending_hr'
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
        elif current_status == 'pending_hr' and ('hr' in roles or 'admin' in roles):
            # HR review
            if decision == 'approved':
                new_status = 'approved'
                action = 'hr_approved'
            else:
                new_status = 'denied'
                action = 'hr_denied'

            update_query = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.applications`
                SET status = @status,
                    hr_reviewer = @reviewer,
                    hr_decision = @decision,
                    hr_notes = @notes,
                    hr_reviewed_at = @now,
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
                name_key,
                CONCAT(First_Name, ' ', Last_Name) as full_name,
                Hire_Date as hire_date,
                DATE_DIFF(CURRENT_DATE(), Hire_Date, DAY) / 365.25 as years_of_service,
                Job_Title as job_title,
                Function as department,
                Site as site
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
