"""Import sabbatical applications from form data."""
from google.cloud import bigquery
from datetime import datetime
import uuid

client = bigquery.Client(project='talent-demo-482004')

applications = [
    {
        'submitted_at': '2025-04-14T16:40:38',
        'employee_email': 'ebunton@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '1/5/2026',
        'start_date': '2026-01-05',
        'date_flexibility': 'Yes',
        'flexibility_explanation': 'I would like to at least get through November. January seems right, but could also start after Thanksgiving and take 8 weeks from there. That may be better for school! So that would be great.',
        'sabbatical_purpose': 'I have worked non stop in education since 1997 and at Firstline since 2001. I really need some extended down time.',
        'coverage_plan': 'I am not sure - a mix of teachers and leaders filling in my gaps. I would be a big part of the planning for this.',
        'manager_discussion': 'Yes',
        'additional_notes': 'Thank you!'
    },
    {
        'submitted_at': '2025-04-14T17:49:38',
        'employee_email': 'cbevans@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '5/19/2026',
        'start_date': '2026-05-19',
        'date_flexibility': 'Yes',
        'flexibility_explanation': '- If anything is happening and the team needs additional support, I would adjust - Eva would finish out the final week of school while I am potentially off; I would not make plans to leave town right at the start of leave or the end of the leave\n- I want to ensure 8th promo is complete and all summer planning products are in a strong place',
        'sabbatical_purpose': 'Rest, renewal and preparation for my final year before my "extended unpaid sabbatical / temporary early retirement time" - I would like time to a. prepare for an out-of-country move, b. give my team time to coordinate things in my absence, c. spend a true summer off with my family before we are separated for an extended time, and d. return refreshed to have an amazing year of academic growth and strong student experiences, etc.',
        'coverage_plan': 'Throughout the upcoming school year, I will create a work plan of tasks and timelines to distribute things that the team may not think of; I will also complete summer work in advance - such as calendars, hiring work, etc, so it will all fall on others. Some of this time will fall over leadership weeks, so I will designate a teammate to share/upload docs and have a weekly+ call/check in for updates on big items.',
        'manager_discussion': 'Yes',
        'additional_notes': 'Part of my plan is to work on cabins/cabanas abroad so leaders can go off to the middle of nowhere and kayak with dolphins and manatees if they need a free or affordable place to retreat to during their leave time!'
    },
    {
        'submitted_at': '2025-04-21T12:52:22',
        'employee_email': 'rcain@firstlineschools.org',
        'sabbatical_option': '12 Weeks - 67% Salary',
        'preferred_dates': '7/15/2027',
        'start_date': '2027-07-15',
        'date_flexibility': 'No',
        'flexibility_explanation': "I am trying to pair this with my husband's one-year sabbatical that he will have in the 2027-28 school year. I think we would likely start sabbatical in July 2027 but it might be August. I would usually take a 3 week vacation in June. I'm thinking about staying in June that year to help cover good vacations for my team - but then wondering about using some of that in the next fiscal to help cover a full 6 month leave of absence, some of which would be sabbatical. I've been discussing with Sabrina for a few years, but wanted to get this officially in a request form.",
        'sabbatical_purpose': 'Rest and Renewal. I would like to take this sooner, but working to pair with my husband\'s upcoming sabbatical and leaving the country for 6 months with our children (they do not yet know).',
        'coverage_plan': 'split among my direct reports, I would detail out a plan. We may need to bring on some additional operational support (someone like Troy) to help take duties off others so they could cover mine.',
        'manager_discussion': 'Yes',
        'additional_notes': 'this is still a couple years away, but as this is first come, first serve, I wanted to be sure the timing works for all involved and am putting request in now for that reason.'
    },
    {
        'submitted_at': '2025-04-23T17:09:27',
        'employee_email': 'spence@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '2/1/2027',
        'start_date': '2027-02-01',
        'date_flexibility': 'Yes',
        'flexibility_explanation': 'I want to fit this sabbatical in the right time frame for the org. My plan would be to take Feb - April, using 4 weeks of vacation time at that point.',
        'sabbatical_purpose': 'Rest / Renewal - Of the cohort of CEOs I started with, most if not all are not in their roles anymore. I need a break.',
        'coverage_plan': 'I will create a detailed plan, and I will need to work with the board. I think this falls in between major duties that I have.',
        'manager_discussion': 'No',
        'additional_notes': ''
    },
    {
        'submitted_at': '2025-04-30T12:03:38',
        'employee_email': 'gguerin@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '9/1/2025',
        'start_date': '2025-09-01',
        'date_flexibility': 'Yes',
        'flexibility_explanation': 'I am flexible with Fall 2025.',
        'sabbatical_purpose': 'Rest and Renewal. 15+ years of tireless hard work with constant changes to programming, staff, rolling with the punches, with Firstlineschools, Nola 180 has been both rewarding and exhausting.',
        'coverage_plan': 'n/a',
        'manager_discussion': 'No',
        'additional_notes': 'Special Education staff needs more recognition for the dedicated hard work we do every day.'
    },
    {
        'submitted_at': '2025-07-09T14:25:47',
        'employee_email': 'sdomango@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '10/20/2025',
        'start_date': '2025-10-20',
        'date_flexibility': 'Yes',
        'flexibility_explanation': "My preference is to connect by weeks with vacation to ensure I'm not gone during the end-of-the-year. I would like my 8 weeks broken down as follows: Start October 20th -31st(2 weeks); November 3-21 (3 weeks) and December 1-19 (3 weeks). This will be a total of 8 weeks. All weeks in between are holiday breaks. My return would be Monday, January 1, 2026.",
        'sabbatical_purpose': 'Spend time with my dad and family. My dad and aunt are ill.',
        'coverage_plan': 'To ensure the success of my department, I will create a scope and sequence outlining responsibilities with suggestions of who on the C-team should support during this time.',
        'manager_discussion': 'Yes',
        'additional_notes': "Thanks for creating this opportunity. As a tenured/veteran employee, it's one of the times (maybe the first) that I truly feel appreciated for my hard work and dedication to this organization. Even if I don't get the opportunity at this moment (I may be submitting late), I'm ready when the opportunity presents itself for me. Add me to the waitlist. Thanks."
    },
    {
        'submitted_at': '2025-08-26T19:38:33',
        'employee_email': 'jaroussell@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '2/23/2026',
        'start_date': '2026-02-23',
        'date_flexibility': 'No',
        'flexibility_explanation': 'I choose these dates to ensure I am back in the building for state testing training and testing.',
        'sabbatical_purpose': 'Rest and Renewal: mission trip; self reflection and direction',
        'coverage_plan': 'My coaching load would be divided between Ms. Fletcher and other school leaders. I have created teacher reflection forms for teachers to complete to serve as an additional coaching support if needed.',
        'manager_discussion': 'Yes',
        'additional_notes': 'This is a great opportunity for staff who have remained at Firstline over the years. I greatly appreciate this becoming an offer to us!'
    },
    {
        'submitted_at': '2025-09-09T14:09:05',
        'employee_email': 'zach@esynola.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '11/2/2026',
        'start_date': '2026-11-02',
        'date_flexibility': 'Yes',
        'flexibility_explanation': "I am a bit flexible. From November to January is a low season for many of my responsibilities. I want to schedule it then, and also take advantage of the 3 weeks of holiday break. November 2, 2026 - January 15, 2027 would be 8 weeks of sabbatical plus 3 weeks of break. I'd be happy to discuss adjustments to my exact start/end dates during that time of year.",
        'sabbatical_purpose': 'Rest and Renewal for sure. Beginning in January 2026 my wife will relocate to Nairobi, Kenya for an 18 month contract. I would use my sabbatical time to be there with her.',
        'coverage_plan': 'I will primarily collaborate with Charlotte on this.',
        'manager_discussion': 'Yes',
        'additional_notes': ''
    },
    {
        'submitted_at': '2025-12-19T16:08:40',
        'employee_email': 'alee@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '10/19/2026',
        'start_date': '2026-10-19',
        'date_flexibility': 'Yes',
        'flexibility_explanation': 'I am considering when the best time would be for me as well as my team.',
        'sabbatical_purpose': 'Complete educational pursuits as well as renewal.',
        'coverage_plan': 'Distributed amongst my leaders',
        'manager_discussion': 'Yes',
        'additional_notes': 'NA'
    },
    {
        'submitted_at': '2026-01-09T14:20:15',
        'employee_email': 'gsextion@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '1/4/2027',
        'start_date': '2027-01-04',
        'date_flexibility': 'Yes',
        'flexibility_explanation': 'After Winter Break',
        'sabbatical_purpose': 'Rest and Renewal',
        'coverage_plan': 'N/A',
        'manager_discussion': 'Yes',
        'additional_notes': ''
    },
    {
        'submitted_at': '2026-01-25T13:32:44',
        'employee_email': 'svenable@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '4/5/2027',
        'start_date': '2027-04-05',
        'date_flexibility': 'Yes',
        'flexibility_explanation': 'I need to take care of my health physical and mental',
        'sabbatical_purpose': 'Rest and renewal',
        'coverage_plan': 'My assistant band director will be covering all my classes and duties',
        'manager_discussion': 'Yes',
        'additional_notes': 'There is nothing else'
    },
    {
        'submitted_at': '2026-01-28T16:30:27',
        'employee_email': 'kbaylor@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '10/19/2026',
        'start_date': '2026-10-19',
        'date_flexibility': 'Yes',
        'flexibility_explanation': 'Totally flexible',
        'sabbatical_purpose': 'To take care of my elderly father. This is a good time to be with my father because he will not have to spend the holidays alone.',
        'coverage_plan': 'N/A',
        'manager_discussion': 'Yes',
        'additional_notes': 'I believe that I have expressed everything in person.'
    },
    {
        'submitted_at': '2026-01-29T07:38:28',
        'employee_email': 'sblouin@firstlineschools.org',
        'sabbatical_option': '8 Weeks - 100% Salary',
        'preferred_dates': '8/19/2030',
        'start_date': '2030-08-19',
        'date_flexibility': 'Yes',
        'flexibility_explanation': 'If needed to change date.....I am ok with it.',
        'sabbatical_purpose': 'Grief.....I inquired about the sabbatical leave 2 months ago and it was said that I did not qualify. I really need it sooner.',
        'coverage_plan': 'Coverage',
        'manager_discussion': 'No',
        'additional_notes': ''
    },
]

# Calculate end dates (8 weeks = 56 days, 12 weeks = 84 days)
from datetime import timedelta

for app in applications:
    if app.get('start_date'):
        start = datetime.strptime(app['start_date'], '%Y-%m-%d')
        weeks = 12 if '12 Weeks' in app['sabbatical_option'] else 8
        end = start + timedelta(days=weeks * 7)
        app['end_date'] = end.strftime('%Y-%m-%d')
        app['leave_weeks'] = weeks
        app['salary_percentage'] = 67 if '67%' in app['sabbatical_option'] else 100

# Lookup staff names from staff_master_list
print("Looking up employee names from staff_master_list...")
staff_lookup = {}
try:
    query = "SELECT employee_email, employee_name, site FROM `talent-demo-482004.staff.staff_master_list`"
    results = client.query(query).result()
    for row in results:
        staff_lookup[row.employee_email.lower()] = {
            'name': row.employee_name,
            'site': row.site
        }
    print(f"Loaded {len(staff_lookup)} staff records")
except Exception as e:
    print(f"Warning: Could not load staff list: {e}")

# Insert into BigQuery using existing table schema
for app in applications:
    app_id = str(uuid.uuid4())[:8].upper()

    # Look up employee info
    email_lower = app['employee_email'].lower()
    staff_info = staff_lookup.get(email_lower, {})
    employee_name = staff_info.get('name', app['employee_email'].split('@')[0])
    site = staff_info.get('site', '')

    query = """
    INSERT INTO `talent-demo-482004.sabbatical.applications` (
        application_id, submitted_at, employee_name, employee_email, site,
        requested_start_date, requested_end_date, leave_weeks, salary_percentage,
        flexible, flexibility_details,
        sabbatical_purpose, why_now, coverage_plan, manager_discussed,
        additional_comments, status, created_at, updated_at
    ) VALUES (
        @application_id, @submitted_at, @employee_name, @employee_email, @site,
        @requested_start_date, @requested_end_date, @leave_weeks, @salary_percentage,
        @flexible, @flexibility_details,
        @sabbatical_purpose, @why_now, @coverage_plan, @manager_discussed,
        @additional_comments, @status, @created_at, @updated_at
    )
    """

    # Parse dates
    submitted_at = datetime.fromisoformat(app['submitted_at'])
    start_date = datetime.strptime(app['start_date'], '%Y-%m-%d').date() if app.get('start_date') else None
    end_date = datetime.strptime(app['end_date'], '%Y-%m-%d').date() if app.get('end_date') else None

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('application_id', 'STRING', app_id),
            bigquery.ScalarQueryParameter('submitted_at', 'TIMESTAMP', submitted_at),
            bigquery.ScalarQueryParameter('employee_name', 'STRING', employee_name),
            bigquery.ScalarQueryParameter('employee_email', 'STRING', app['employee_email']),
            bigquery.ScalarQueryParameter('site', 'STRING', site),
            bigquery.ScalarQueryParameter('requested_start_date', 'DATE', start_date),
            bigquery.ScalarQueryParameter('requested_end_date', 'DATE', end_date),
            bigquery.ScalarQueryParameter('leave_weeks', 'INT64', app.get('leave_weeks', 8)),
            bigquery.ScalarQueryParameter('salary_percentage', 'INT64', app.get('salary_percentage', 100)),
            bigquery.ScalarQueryParameter('flexible', 'BOOL', app['date_flexibility'] == 'Yes'),
            bigquery.ScalarQueryParameter('flexibility_details', 'STRING', app.get('flexibility_explanation', '')),
            bigquery.ScalarQueryParameter('sabbatical_purpose', 'STRING', app['sabbatical_purpose']),
            bigquery.ScalarQueryParameter('why_now', 'STRING', ''),
            bigquery.ScalarQueryParameter('coverage_plan', 'STRING', app['coverage_plan']),
            bigquery.ScalarQueryParameter('manager_discussed', 'BOOL', app['manager_discussion'] == 'Yes'),
            bigquery.ScalarQueryParameter('additional_comments', 'STRING', app.get('additional_notes', '')),
            bigquery.ScalarQueryParameter('status', 'STRING', 'Submitted'),
            bigquery.ScalarQueryParameter('created_at', 'TIMESTAMP', submitted_at),
            bigquery.ScalarQueryParameter('updated_at', 'TIMESTAMP', datetime.now()),
        ]
    )

    try:
        client.query(query, job_config=job_config).result()
        print(f'{app_id}: {employee_name} ({app["employee_email"]}) - {app["leave_weeks"]} weeks - Start: {app["start_date"]}')
    except Exception as e:
        print(f'ERROR: {app["employee_email"]} - {e}')

print()
print(f'Done! Imported {len(applications)} applications.')
