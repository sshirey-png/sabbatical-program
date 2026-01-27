"""
Setup BigQuery tables for Sabbatical Program
Run this once to create the required tables
"""

from google.cloud import bigquery

PROJECT_ID = 'talent-demo-482004'
DATASET_ID = 'sabbatical'

client = bigquery.Client(project=PROJECT_ID)

# Create dataset if it doesn't exist
dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
dataset_ref.location = "US"

try:
    client.create_dataset(dataset_ref)
    print(f"Created dataset {DATASET_ID}")
except Exception as e:
    print(f"Dataset exists or error: {e}")

# Create applications table
applications_schema = [
    bigquery.SchemaField("application_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("employee_name_key", "STRING"),
    bigquery.SchemaField("employee_name", "STRING"),
    bigquery.SchemaField("employee_email", "STRING"),
    bigquery.SchemaField("hire_date", "DATE"),
    bigquery.SchemaField("years_of_service", "FLOAT64"),
    bigquery.SchemaField("job_title", "STRING"),
    bigquery.SchemaField("department", "STRING"),
    bigquery.SchemaField("site", "STRING"),
    bigquery.SchemaField("requested_start_date", "DATE"),
    bigquery.SchemaField("requested_end_date", "DATE"),
    bigquery.SchemaField("duration_weeks", "INT64"),
    bigquery.SchemaField("sabbatical_purpose", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("submitted_at", "TIMESTAMP"),
    bigquery.SchemaField("talent_reviewer", "STRING"),
    bigquery.SchemaField("talent_decision", "STRING"),
    bigquery.SchemaField("talent_notes", "STRING"),
    bigquery.SchemaField("talent_reviewed_at", "TIMESTAMP"),
    bigquery.SchemaField("hr_reviewer", "STRING"),
    bigquery.SchemaField("hr_decision", "STRING"),
    bigquery.SchemaField("hr_notes", "STRING"),
    bigquery.SchemaField("hr_reviewed_at", "TIMESTAMP"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
]

applications_table = bigquery.Table(
    f"{PROJECT_ID}.{DATASET_ID}.applications",
    schema=applications_schema
)

try:
    client.create_table(applications_table)
    print("Created applications table")
except Exception as e:
    print(f"Applications table exists or error: {e}")

# Create approval_history table
history_schema = [
    bigquery.SchemaField("history_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("application_id", "STRING"),
    bigquery.SchemaField("action", "STRING"),
    bigquery.SchemaField("actor_email", "STRING"),
    bigquery.SchemaField("actor_name", "STRING"),
    bigquery.SchemaField("notes", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

history_table = bigquery.Table(
    f"{PROJECT_ID}.{DATASET_ID}.approval_history",
    schema=history_schema
)

try:
    client.create_table(history_table)
    print("Created approval_history table")
except Exception as e:
    print(f"Approval history table exists or error: {e}")

# Create notifications_log table
notifications_schema = [
    bigquery.SchemaField("notification_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("application_id", "STRING"),
    bigquery.SchemaField("recipient_email", "STRING"),
    bigquery.SchemaField("notification_type", "STRING"),
    bigquery.SchemaField("sent_at", "TIMESTAMP"),
    bigquery.SchemaField("status", "STRING"),
]

notifications_table = bigquery.Table(
    f"{PROJECT_ID}.{DATASET_ID}.notifications_log",
    schema=notifications_schema
)

try:
    client.create_table(notifications_table)
    print("Created notifications_log table")
except Exception as e:
    print(f"Notifications table exists or error: {e}")

print("\nSetup complete!")
print(f"Tables created in {PROJECT_ID}.{DATASET_ID}")
