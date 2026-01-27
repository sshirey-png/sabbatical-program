"""
Migration script to update sabbatical.applications table schema
Adds new columns for expanded form fields and 3-stage approval workflow
"""

from google.cloud import bigquery

PROJECT_ID = 'talent-demo-482004'
DATASET_ID = 'sabbatical'

def run_migration():
    client = bigquery.Client(project=PROJECT_ID)

    # New columns to add
    migrations = [
        # Leave options
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS leave_weeks INT64",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS salary_percentage INT64",

        # Additional form fields
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS why_now STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS coverage_plan STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS flexible BOOL",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS flexibility_details STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS manager_discussed BOOL",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS additional_comments STRING",

        # Director approval fields (new first stage)
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS director_reviewer STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS director_decision STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS director_notes STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS director_reviewed_at TIMESTAMP",

        # CEO approval fields (new final stage)
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS ceo_reviewer STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS ceo_decision STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS ceo_notes STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS ceo_reviewed_at TIMESTAMP",

        # Supervisor info for director routing
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS supervisor_name STRING",
        "ALTER TABLE `{project}.{dataset}.applications` ADD COLUMN IF NOT EXISTS supervisor_email STRING",
    ]

    for sql in migrations:
        query = sql.format(project=PROJECT_ID, dataset=DATASET_ID)
        print(f"Running: {query[:80]}...")
        try:
            client.query(query).result()
            print("  OK")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("  Already exists, skipping")
            else:
                print(f"  Error: {e}")

    print("\nMigration complete!")

    # Show current schema
    print("\nCurrent table schema:")
    table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.applications")
    for field in table.schema:
        print(f"  {field.name}: {field.field_type}")

if __name__ == '__main__':
    run_migration()
