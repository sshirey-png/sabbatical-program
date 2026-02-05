# Sabbatical Program Documentation

This folder contains documentation for the FirstLine Schools Sabbatical Program application.

## Live Application

**URL:** https://sabbatical-program-965913991496.us-central1.run.app

## Permission Structure (3 Levels)

The Sabbatical Program has a simplified 3-level permission structure:

### Level 1: Application Page
**Who:** Everyone in the organization
**Access:** Can view the application page and apply for sabbatical

### Level 2: Admin Dashboard
**Who:** Network Admins and School Leaders
**Access:** View and manage sabbatical applications

| Role | Access Level |
|------|--------------|
| C-Team (CEO, CPO, CHR, CXO) | All schools (Network) |
| ExDir of Teaching and Learning (kfeil) | All schools (Network) |
| HR/Talent Team | All schools (Network) |
| School Directors/Principals | Their school only |
| Assistant Principals | Their school only |
| Heads of School | Their school only |

**Network Admins can:**
- View all applications across all schools
- Change application status
- Delete applications
- Approve/deny date change requests

**School-Level Admins can:**
- View applications from their school only
- Cannot change status or delete (read-only)

### Level 3: My Sabbatical Page
**Who:** Employees with approved sabbaticals and their supervisor chain
**Access:** Planning and progress tracking

- Employees can see their own sabbatical plan once tentatively approved
- Supervisors can view their direct reports' sabbatical plans
- Accessible via the Supervisor Dashboard link

## Quick Links

- [Application Page](https://sabbatical-program-965913991496.us-central1.run.app/)
- [My Sabbatical](https://sabbatical-program-965913991496.us-central1.run.app/my-sabbatical)
- [Approvals](https://sabbatical-program-965913991496.us-central1.run.app/approvals)

## Configuration

### Network Admins (Full Access)

Edit `SABBATICAL_NETWORK_ADMINS` in `app.py`:

```python
SABBATICAL_NETWORK_ADMINS = [
    'sshirey@firstlineschools.org',      # Chief People Officer
    'brichardson@firstlineschools.org',  # Chief of Human Resources
    'spence@firstlineschools.org',       # CEO
    'sdomango@firstlineschools.org',     # Chief Experience Officer
    'talent@firstlineschools.org',
    'hr@firstlineschools.org',
    'kfeil@firstlineschools.org',        # ExDir of Teaching and Learning
    # ... add more as needed
]
```

### School Leader Titles (School-Level Access)

Edit `SABBATICAL_SCHOOL_LEADER_TITLES` in `app.py`:

```python
SABBATICAL_SCHOOL_LEADER_TITLES = [
    'school director',
    'principal',
    'assistant principal',
    'head of school',
]
```

School leaders are automatically detected by matching their job title against this list. Their school access is determined by their `Location_Name` in the staff database.

## Contact

For questions or access issues:
- Scott Shirey (sshirey@firstlineschools.org) - Chief People Officer
- Brittney Richardson (brichardson@firstlineschools.org) - Chief of Human Resources
