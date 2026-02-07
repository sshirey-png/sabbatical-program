# FirstLine Schools Sabbatical Program - Technical Guide

## Overview

This guide is for developers and administrators who need to maintain, update, or extend the sabbatical program application. It covers the codebase structure, deployment process, and common maintenance tasks.

---

## Table of Contents

1. [Getting Started with GitHub](#1-getting-started-with-github)
2. [Project Structure](#2-project-structure)
3. [Local Development Setup](#3-local-development-setup)
4. [Making Changes](#4-making-changes)
5. [Deployment](#5-deployment)
6. [Common Maintenance Tasks](#6-common-maintenance-tasks)
7. [Understanding the Permission System](#7-understanding-the-permission-system)
8. [Database Schema](#8-database-schema)
9. [Troubleshooting](#9-troubleshooting)
10. [Using Claude Code](#10-using-claude-code)

---

## 1. Getting Started with GitHub

### Repository Location
```
https://github.com/sshirey-png/sabbatical-program
```

### Accessing the Code

#### Option A: View Online (No Installation)
1. Go to https://github.com/sshirey-png/sabbatical-program
2. Click on any file to view its contents
3. Use the file browser to navigate

#### Option B: Clone with Git (Recommended for Making Changes)
```bash
git clone https://github.com/sshirey-png/sabbatical-program.git
cd sabbatical-program
```

---

## 2. Project Structure

```
sabbatical-program/
├── app.py                    # Main Flask application (all backend logic)
├── index.html                # Application page (apply, calendar, admin)
├── my-sabbatical.html        # Planning page for approved sabbaticals
├── approvals.html            # Supervisor approval interface
├── requirements.txt          # Python dependencies
├── Dockerfile                # Container build instructions
├── setup_bigquery.py         # Initial table setup script
├── migrate_schema.py         # Schema migration scripts
└── docs/                     # Documentation
    ├── README.md
    ├── USER_GUIDE.md
    └── TECHNICAL_GUIDE.md
```

### Key Files Explained

| File | Purpose | When to Edit |
|------|---------|--------------|
| `app.py` | All backend routes, auth, BigQuery queries | Adding features, changing logic |
| `index.html` | Main application page UI | Changing staff/admin interface |
| `my-sabbatical.html` | Planning page UI | Changing planning features |
| `approvals.html` | Approval workflow UI | Changing approval interface |
| `Dockerfile` | Container build | Adding new dependencies |

---

## 3. Local Development Setup

### Prerequisites
- Python 3.9 or higher
- Google Cloud SDK (gcloud CLI)
- Git (optional but recommended)

### Step-by-Step Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/sshirey-png/sabbatical-program.git
   cd sabbatical-program
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv

   # Windows:
   venv\Scripts\activate

   # Mac/Linux:
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up Google Cloud credentials**
   ```bash
   gcloud auth application-default login
   ```

5. **Run locally**
   ```bash
   # Set development mode (bypasses OAuth)
   set FLASK_ENV=development   # Windows
   export FLASK_ENV=development  # Mac/Linux

   python app.py
   ```

6. **Open in browser**
   ```
   http://localhost:8080
   ```

### Development Mode
When `FLASK_ENV=development`, the app:
- Skips Google OAuth (auto-logs in as dev user)
- Shows detailed error messages
- Dev user is set in `app.py` as `DEV_USER_EMAIL`

---

## 4. Making Changes

### Workflow Overview

```
1. Make changes locally
2. Test locally
3. Commit to Git
4. Push to GitHub
5. Deploy to Cloud Run
```

### Example: Adding a New Network Admin

1. **Open `app.py`**

2. **Find the `SABBATICAL_NETWORK_ADMINS` list** (around line 50)

3. **Add the new email:**
   ```python
   SABBATICAL_NETWORK_ADMINS = [
       'sshirey@firstlineschools.org',
       'brichardson@firstlineschools.org',
       # ... existing emails ...
       'newemail@firstlineschools.org',  # New Person - Title
   ]
   ```

4. **Save the file**

5. **Commit and push:**
   ```bash
   git add app.py
   git commit -m "Add [Name] to sabbatical admin list"
   git push origin master
   ```

6. **Deploy** (see Section 5)

---

## 5. Deployment

### Where It Runs
The application runs on **Google Cloud Run** in the `talent-demo-482004` project.

### Deployment Command

From the project directory:
```bash
gcloud run deploy sabbatical-program \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

### What Happens During Deployment
1. Google Cloud builds a Docker container from your code
2. The container is pushed to Google Container Registry
3. Cloud Run creates a new revision
4. Traffic is routed to the new revision
5. Old revisions are kept (can rollback if needed)

### Deployment Takes ~3-5 Minutes
When done, you'll see:
```
Service URL: https://sabbatical-program-965913991496.us-central1.run.app
```

### Verifying Deployment
1. Open the Service URL
2. Hard refresh (Ctrl+Shift+R) to clear cache
3. Test the feature you changed

### Rolling Back
If something breaks, you can rollback in Google Cloud Console:
1. Go to https://console.cloud.google.com
2. Navigate to Cloud Run → sabbatical-program
3. Click "Revisions" tab
4. Select a previous revision → "Manage Traffic" → Route 100% to it

---

## 6. Common Maintenance Tasks

### Adding a Network Admin

**Note:** C-Team members are auto-detected by job title. Anyone with "Chief" or "Ex Dir" in their title automatically gets network admin access - no manual addition needed.

For non-C-Team admins (HR/Talent team, etc.):

**File:** `app.py`
**Location:** `SABBATICAL_NETWORK_ADMINS` list

```python
SABBATICAL_NETWORK_ADMINS = [
    # ... existing emails ...
    'newadmin@firstlineschools.org',  # Name - Title
]
```

Then commit, push, and deploy.

### Adding a School Leader Title

**File:** `app.py`
**Location:** `SABBATICAL_SCHOOL_LEADER_TITLES` list

```python
SABBATICAL_SCHOOL_LEADER_TITLES = [
    'school director',
    'principal',
    'assistant principal',
    'head of school',
    'new title here',  # Add new title (lowercase)
]
```

### Updating Sabbatical Options

**File:** `app.py`
**Location:** `SABBATICAL_OPTIONS` dictionary

```python
SABBATICAL_OPTIONS = {
    '8_weeks_100': {'weeks': 8, 'salary_percentage': 100, 'label': '8 weeks at 100% salary'},
    '12_weeks_70': {'weeks': 12, 'salary_percentage': 70, 'label': '12 weeks at 70% salary'},
}
```

### Adding an Email Alias

For users with multiple email addresses:

**File:** `app.py`
**Location:** `EMAIL_ALIASES` dictionary

```python
EMAIL_ALIASES = {
    'alias@otherdomain.org': 'primary@firstlineschools.org',
}
```

### Modifying the Planning Checklist

The default checklist items are defined in `app.py` in the `DEFAULT_CHECKLIST` constant. Each item has:
- `id`: Unique identifier
- `text`: Display text
- `phase`: 'before', 'during', or 'returning'

---

## 7. Understanding the Permission System

### Three Permission Levels

```
Level 1: Application Page
├── Who: Everyone in the organization
└── Access: Can view and apply for sabbatical

Level 2: Admin Dashboard
├── Who: Network Admins and School Leaders
└── Access: View and manage sabbatical applications

Level 3: My Sabbatical Page
├── Who: Employees with approved sabbaticals + supervisors
└── Access: Planning and progress tracking
```

### Permission Check Flow

```python
def get_sabbatical_admin_access(email):
    # 1. Check if network admin
    if email in SABBATICAL_NETWORK_ADMINS:
        return {'level': 'network', 'schools': None}  # All schools

    # 2. Check if school leader (by job title)
    if job_title in SABBATICAL_SCHOOL_LEADER_TITLES:
        return {'level': 'school', 'schools': [their_school]}

    # 3. No admin access
    return None
```

### Key Permission Functions in `app.py`

| Function | Purpose |
|----------|---------|
| `get_sabbatical_admin_access(email)` | Check admin permissions |
| `require_network_admin` | Decorator for network-admin-only routes |
| `get_supervisor_chain(email)` | Get supervisor hierarchy for approvals |
| `resolve_email_alias(email)` | Map alias emails to primary |

---

## 8. Database Schema

### BigQuery Project and Dataset
```
Project: talent-demo-482004
Dataset: sabbatical
```

### Tables

#### `applications` - Main sabbatical applications
```sql
application_id STRING (Primary Key)
employee_name STRING
employee_email STRING
employee_name_key STRING
employee_location STRING
hire_date DATE
years_of_service FLOAT64
status STRING
leave_weeks INT64
salary_percentage INT64
start_date DATE
end_date DATE
created_at TIMESTAMP
updated_at TIMESTAMP
admin_notes STRING
```

#### `checklist_items` - Planning checklist
```sql
id STRING (Primary Key)
application_id STRING (Foreign Key)
task_id STRING
task_text STRING
phase STRING ('before', 'during', 'returning')
completed BOOL
completed_at TIMESTAMP
completed_by STRING
notes STRING
```

#### `coverage_assignments` - Coverage plan
```sql
id STRING (Primary Key)
application_id STRING (Foreign Key)
coverage_name STRING
responsibilities STRING
created_at TIMESTAMP
updated_at TIMESTAMP
```

#### `plan_links` - Linked documents
```sql
id STRING (Primary Key)
application_id STRING (Foreign Key)
title STRING
url STRING
created_at TIMESTAMP
```

#### `activity_history` - Activity log
```sql
id STRING (Primary Key)
application_id STRING (Foreign Key)
timestamp TIMESTAMP
user_email STRING
user_name STRING
action STRING
description STRING
```

#### `date_change_requests` - Date change requests
```sql
id STRING (Primary Key)
application_id STRING (Foreign Key)
requested_by STRING
requested_at TIMESTAMP
old_start_date DATE
old_end_date DATE
new_start_date DATE
new_end_date DATE
reason STRING
status STRING ('Pending', 'Approved', 'Denied')
talent_approved BOOL
talent_approved_by STRING
talent_approved_at TIMESTAMP
```

#### `plan_approvals` - Plan approval workflow
```sql
id STRING (Primary Key)
application_id STRING (Foreign Key)
approver_email STRING
approver_name STRING
approver_role STRING
status STRING ('Pending', 'Approved', 'Changes Requested')
notes STRING
approved_at TIMESTAMP
```

#### `messages` - Planning messages/notes
```sql
id STRING (Primary Key)
application_id STRING (Foreign Key)
from_email STRING
from_name STRING
message STRING
created_at TIMESTAMP
```

---

## 9. Troubleshooting

### Deployment Fails

**"Permission denied"**
```bash
gcloud auth login
gcloud config set project talent-demo-482004
```

**"Docker build failed"**
- Check `requirements.txt` for typos
- Check `Dockerfile` syntax

### Application Errors

**500 Internal Server Error**
- Check Cloud Run logs:
  ```bash
  gcloud run logs read sabbatical-program --region us-central1
  ```

**BigQuery Permission Denied**
- The Cloud Run service account needs BigQuery access
- Check IAM permissions in Google Cloud Console

### Local Development Issues

**"Module not found"**
```bash
pip install -r requirements.txt
```

**"OAuth error" locally**
- Make sure `FLASK_ENV=development` is set
- Or set up OAuth credentials for local testing

### Common Runtime Errors

**"name 'user_email' is not defined"**
- A route handler is missing user session retrieval
- Add at start of function:
  ```python
  user = session.get('user')
  if not user:
      return jsonify({'error': 'Authentication required'}), 401
  user_email = user.get('email', '')
  ```

---

## 10. Using Claude Code

Claude Code is an AI assistant that helped build this system. You can use it to make changes without deep coding knowledge.

### Installing Claude Code
```bash
npm install -g @anthropic-ai/claude-code
```

### Basic Usage
```bash
cd sabbatical-program
claude

# Then type your request, for example:
> Add jsmith@firstlineschools.org as a network admin

# Claude will:
# 1. Find the right file
# 2. Make the change
# 3. Show you what changed
# 4. Offer to commit and deploy
```

### Example Prompts

**Adding an admin:**
```
Add jsmith@firstlineschools.org (John Smith - Director of Operations) as a sabbatical admin
```

**Fixing a bug:**
```
I'm getting an error "user_email not defined" when approving date changes
```

**Making UI changes:**
```
Add a "Back to Admin" button in the header on the my-sabbatical page
```

**Deploying:**
```
Deploy the changes to Cloud Run
```

### Tips for Using Claude Code
1. Be specific about what you want
2. Mention file names if you know them
3. Ask it to explain before making changes
4. Always test after deployment
5. Say "commit and push" when ready to save to GitHub

---

## Quick Reference

### Important URLs

| Resource | URL |
|----------|-----|
| Live Application | https://sabbatical-program-965913991496.us-central1.run.app |
| GitHub Repository | https://github.com/sshirey-png/sabbatical-program |
| Google Cloud Console | https://console.cloud.google.com/run?project=talent-demo-482004 |
| BigQuery Console | https://console.cloud.google.com/bigquery?project=talent-demo-482004 |

### Common Commands

```bash
# Deploy
gcloud run deploy sabbatical-program --source . --region us-central1 --allow-unauthenticated

# View logs
gcloud run logs read sabbatical-program --region us-central1

# Git commit and push
git add .
git commit -m "Description of change"
git push origin master

# Run locally
set FLASK_ENV=development
python app.py
```

### Key Configuration Locations

| Setting | File | Variable/Section |
|---------|------|------------------|
| Network admins | app.py | SABBATICAL_NETWORK_ADMINS |
| School leader titles | app.py | SABBATICAL_SCHOOL_LEADER_TITLES |
| Sabbatical options | app.py | SABBATICAL_OPTIONS |
| Email aliases | app.py | EMAIL_ALIASES |
| BigQuery settings | app.py | PROJECT_ID, DATASET_ID |
| Default checklist | app.py | DEFAULT_CHECKLIST |

---

## Support

For technical issues:
1. Check the troubleshooting section above
2. Review Cloud Run logs for error messages
3. Use Claude Code to help diagnose and fix issues
4. Check GitHub issues or create a new one

For access/permission issues:
- Contact Scott Shirey (sshirey@firstlineschools.org)
- Contact Brittney Richardson (brichardson@firstlineschools.org)
