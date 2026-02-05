# FirstLine Schools Sabbatical Program - User Guide

## Overview

The Sabbatical Program honors long-tenured employees (10+ years of service) with paid sabbatical leave. This web application allows eligible staff to apply, track their application, and plan their sabbatical once approved.

**Live URL:** https://sabbatical-program-965913991496.us-central1.run.app

---

## Available Pages

| Page | URL Path | Purpose |
|------|----------|---------|
| Application Page | `/` | Apply for sabbatical, view calendar |
| My Sabbatical | `/my-sabbatical` | Planning and progress tracking (approved applicants) |
| Approvals | `/approvals` | Review and approve applications (supervisors) |

---

## 1. Application Page (Staff View)

### Who Can Access
- All staff with a @firstlineschools.org Google account

### Eligibility
- Must have **10+ years of service** at FirstLine Schools
- System automatically calculates years of service from hire date

### Applying for Sabbatical

1. **Log in** with your @firstlineschools.org Google account
2. Your information will auto-populate from staff records
3. Select your **sabbatical option**:
   - 8 weeks at 100% salary
   - 12 weeks at 70% salary
4. Choose your **preferred dates** using the calendar
5. Click **Submit Application**

### Calendar View

The calendar shows:
- **Your requested dates** (if you have an application)
- **Other approved sabbaticals** (to help avoid scheduling conflicts)
- Toggle between **Month** and **School Year** (July-June) views

### Application Status

After submitting, your application moves through these stages:

| Status | Meaning |
|--------|---------|
| Applied | Application submitted, awaiting supervisor review |
| Tentatively Approved | Supervisor approved, awaiting HR review |
| Approved | Fully approved, ready for planning |
| Planning | Actively working on sabbatical plan |
| Plan Submitted | Plan submitted for final approval |
| Confirmed | Everything approved, sabbatical confirmed |
| On Sabbatical | Currently on sabbatical leave |
| Returning | Returning from sabbatical |
| Completed | Sabbatical finished |

---

## 2. My Sabbatical Page (Planning)

### Who Can Access
- Staff with an approved (or tentatively approved) sabbatical application
- Supervisors can view their direct reports' plans
- Admins can view anyone's plan

### Features

#### Planning Checklist
A comprehensive checklist organized by phase:
- **Before Sabbatical**: Tasks to complete before you leave
- **During Sabbatical**: Reminders for while you're away
- **Returning**: Tasks for your return

Check off items as you complete them. Add notes to any task by clicking the notes icon.

#### Coverage Plan
Document who will cover your responsibilities while you're away:
- Add coverage assignments with name and responsibilities
- Edit or remove assignments as needed

#### Plan Documents
Link to important documents for your sabbatical:
- Google Docs, Sheets, or other planning documents
- Add titles and URLs for easy reference

#### Activity History
View all activity on your sabbatical:
- Status changes
- Plan submissions
- Approvals and feedback

#### Requesting Date Changes
If you need to change your sabbatical dates after approval:
1. Click **"Request Date Change"** on the header card
2. Enter new start and end dates
3. Provide a reason for the change
4. Submit for admin approval

### Plan Submission Workflow

1. Complete your planning checklist (recommended)
2. Add coverage assignments
3. Link your plan documents
4. Click **"Submit Plan for Approval"**
5. Your supervisor chain reviews and approves
6. Once all approvers sign off, status changes to "Confirmed"

### Viewing as Admin
When admins view someone else's sabbatical planning page:
- A **"Back to Admin"** button appears in the header
- A purple banner shows whose plan you're viewing
- Click the button to return to the admin dashboard

---

## 3. Approvals Page (Supervisors & Admins)

### Who Can Access

| Role | Access Level |
|------|--------------|
| Network Admins (C-Team, HR, Talent) | All applications, full control |
| School Directors/Principals | Their school only (read-only) |
| Supervisors | Their direct reports' applications |

### Network Admin Features

**Dashboard View:**
- See all sabbatical applications across the network
- Filter by status, school, or search by name
- View summary statistics

**Application Management:**
- Change application status
- Delete applications
- Resend confirmation emails
- View and approve date change requests

**Calendar View:**
- Toggle between Staff View (your own sabbatical) and Admin View (all sabbaticals)
- See all approved sabbaticals across the organization
- Identify potential scheduling conflicts

### Approving Date Change Requests
When an employee requests a date change:
1. You'll receive an email notification
2. Go to Approvals page
3. Review the request details
4. Click **Approve** or **Deny**

### Plan Approval Workflow
When an employee submits their plan:
1. Supervisor chain members receive email notifications
2. Go to Approvals page or click link in email
3. Review the employee's plan (checklist, coverage, documents)
4. Click **Approve** or **Request Changes**
5. If changes requested, employee revises and resubmits

---

## Permission System

### Network Admins (Full Access)

| Email | Name | Role |
|-------|------|------|
| sshirey@firstlineschools.org | Scott Shirey | Chief People Officer |
| brichardson@firstlineschools.org | Brittney Richardson | Chief of Human Resources |
| spence@firstlineschools.org | Sabrina Pence | CEO |
| sdomango@firstlineschools.org | Sivi Domango | Chief Experience Officer |
| talent@firstlineschools.org | Talent Team | |
| hr@firstlineschools.org | HR Team | |
| kfeil@firstlineschools.org | K. Feil | ExDir of Teaching and Learning |

### School-Level Access
School leaders automatically get read-only access to applications from their school:
- School Director
- Principal
- Assistant Principal
- Head of School

### Supervisor Access
- Supervisors can view and approve their direct reports' sabbatical plans
- Determined by supervisor chain in staff database

---

## Email Notifications

The system sends automatic email notifications for:

| Event | Recipients |
|-------|------------|
| New application submitted | Employee, Talent Team, Supervisor chain |
| Application approved | Employee, Supervisor chain |
| Application denied | Employee |
| Date change requested | Talent/HR Admins |
| Date change approved/denied | Employee, Supervisor chain |
| Plan submitted for approval | Supervisor chain |
| Plan approved | Employee |
| Changes requested | Employee |

---

## Common Tasks

### Checking Your Application Status
1. Go to the application page (`/`)
2. Log in with your Google account
3. Your current application status appears at the top

### Viewing Your Sabbatical Plan
1. Go to My Sabbatical (`/my-sabbatical`)
2. Log in with your Google account
3. View your checklist, coverage plan, and documents

### Approving a Direct Report's Plan (Supervisors)
1. Click the approval link in your email notification
2. Or go to Approvals (`/approvals`)
3. Review the plan details
4. Click Approve or Request Changes

### Switching Between Staff and Admin Views (Admins)
On the main application page:
1. Look for the toggle buttons in the header: **Staff View** | **Admin View**
2. Staff View shows your own application
3. Admin View shows all applications and the full calendar

---

## Troubleshooting

### "You don't have an approved sabbatical"
- Your application may still be pending approval
- Check your application status on the main page
- Contact HR if you believe this is an error

### Can't See Approvals Page
You need one of:
- Network admin access
- School leader title
- Direct reports with sabbatical applications

### Date Change Request Not Appearing
- Date change requests require admin approval
- Check with Talent/HR for status

### Page Won't Load
- Try a hard refresh: Ctrl+Shift+R (Windows) or Cmd+Shift+R (Mac)
- Clear browser cache
- Try a different browser

---

## School Codes Reference

| Code | Full Name |
|------|-----------|
| Ashe | Arthur Ashe Charter School |
| LHA | Langston Hughes Academy |
| Wheatley | Phillis Wheatley Community School |
| Green | Samuel J. Green Charter School |

---

## Contact

For questions or issues:
- Scott Shirey (sshirey@firstlineschools.org) - Chief People Officer
- Brittney Richardson (brichardson@firstlineschools.org) - Chief of Human Resources
- talent@firstlineschools.org - Talent Team
