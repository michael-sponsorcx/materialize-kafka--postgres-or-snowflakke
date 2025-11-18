# Materialize → Kafka → Postgres Pipeline Operations Guide

## Table of Contents
1. [Bulk Deleting Kafka Topics](#bulk-deleting-kafka-topics)
2. [Bulk Deleting Schema Registry Schemas](#bulk-deleting-schema-registry-schemas)
3. [Dropping All Postgres Tables](#dropping-all-postgres-tables)
4. [Key Issues and Solutions](#key-issues-and-solutions)
5. [Important Configuration Notes](#important-configuration-notes)

---

## Bulk Deleting Kafka Topics

### Create File with Topic Names
```bash
cat > topics_to_delete.txt <<EOF
activities
activities_by_manager
agreement_revenue
contacts
dim_accounts
dim_activities
dim_agreement_inventories
dim_agreement_stage_change
dim_agreements
dim_billing_records
dim_fiscal_years
dim_inventory_units
dim_objective_key_results
dim_objectives
dim_organization_categories
dim_organization_types
dim_organizations
dim_properties
dim_trade
dim_trade_collections
dim_users
expiring_agreements
fact_agreement_stages
fact_billing
fact_goal_organization
fact_goal_user
fact_inventory
fact_inventory_lifecycle
fact_objective_key_results
fact_objectives
fact_property_goal
fact_revenue_first_season
fact_trade
fact_trade_collections
fulfillment_by_type
inventory_rate_analysis
permissions_org
permissions_property_and_org_admin
permissions_user
permissions_user_and_org_admin
EOF
```

### Delete All Topics
```bash
while read topic; do
  confluent kafka topic delete "$topic" --force
done < topics_to_delete.txt
```

---

## Bulk Deleting Schema Registry Schemas

### Prerequisites
- Schema Registry API Key and Secret (separate from Kafka cluster credentials)
- Schema Registry endpoint URL

### Set Variables
```bash
SR_ENDPOINT="https://psrc-z6ew6wr.us-east-2.aws.confluent.cloud"
SR_API_KEY="your-schema-registry-api-key"
SR_API_SECRET="your-schema-registry-api-secret"
```

### Delete All Schemas (macOS Compatible)
```bash
confluent schema-registry subject list | tail -n +2 | while read subject; do
  echo "Deleting $subject..."
  response=$(curl -s -w "\n%{http_code}" -X DELETE "$SR_ENDPOINT/subjects/$subject?permanent=true" \
    -u "$SR_API_KEY:$SR_API_SECRET")
  http_code=$(echo "$response" | tail -1)
  body=$(echo "$response" | sed '$d')
  
  if [ "$http_code" = "200" ]; then
    echo "✓ Successfully deleted $subject"
  else
    echo "✗ Failed to delete $subject (HTTP $http_code): $body"
  fi
done
```

### Important Notes on Schema Registry API Keys
- Create API keys specifically for Schema Registry in Confluent Cloud UI
- Navigate to: Schema Registry → API credentials → Add key
- Scope: Select "Schema Registry" (not "Kafka cluster")
- Kafka cluster API keys will NOT work for Schema Registry operations

---

## Dropping All Postgres Tables

### Drop All Tables in Public Schema
```sql
DO $ 
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $;
```

### Alternative: Check Active Connections First
```sql
-- Check for active connections blocking the drop
SELECT pid, usename, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'sponsorcxdb';

-- Terminate connections if needed (be careful!)
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'sponsorcxdb'
  AND pid <> pg_backend_pid();

-- Then drop tables
DO $ 
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $;
```

---

## Key Issues and Solutions

### Issue 1: Nullable Key Fields Break UPSERT

#### Problem
When composite key fields can be NULL, the Postgres connector fails to perform UPSERT operations and falls back to plain INSERT, causing duplicate key violations.

**Symptoms:**
- Connector error: `BatchUpdateException: Batch entry 0 INSERT INTO...`
- Error shows plain `INSERT INTO` instead of `INSERT ... ON CONFLICT`
- Connector configured with `insert.mode: "upsert"` but still using INSERT

#### How to Diagnose
Check the key schema in Schema Registry:
```bash
confluent schema-registry subject describe activities_by_manager_v2-key --version latest
```

Look for nullable fields in the key:
```json
{
  "fields": [
    {
      "name": "activity_id",
      "type": "int"
    },
    {
      "name": "manager_id",
      "type": [
        "null",
        "long"
      ]
    }
  ],
  "name": "row",
  "type": "record"
}
```

The `"type": ["null", "long"]` for `manager_id` indicates it's nullable. This breaks UPSERT.

#### Solution: Filter Out NULLs in Materialized View

**Example for activities_by_manager:**
```sql
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.activities_by_manager_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.activities_by_manager_mv AS
SELECT
  activity_id,
  activity_name,
  date,
  activity_priority,
  activity_completed,
  notes,
  agreement_id,
  account_id,
  account_name,
  activity_type,
  contact_name,
  manager_id,
  manager_name,
  org_name,
  created_at,
  updated_at,
  org_id,
  property_id
FROM materialize.public_dbt.activities_by_manager
WHERE manager_id IS NOT NULL;

-- Recreate the sink
DROP SINK IF EXISTS activities_by_manager_sink CASCADE;
CREATE SINK activities_by_manager_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.activities_by_manager_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'activities_by_manager_v2')
KEY (activity_id, manager_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;
```

**Example for contacts:**
```sql
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.contacts_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.contacts_mv AS
SELECT
  id,
  property_id,
  account_id,
  org_id,
  first_name,
  last_name,
  title,
  birth_month,
  birth_day,
  email,
  phone,
  street_address,
  city,
  state,
  zip,
  archived,
  is_primary_contact,
  is_fulfillment_contact,
  is_billing_contact,
  custom_fields
FROM materialize.public_dbt.contacts
WHERE property_id IS NOT NULL;

-- Recreate the sink
DROP SINK IF EXISTS contacts_sink CASCADE;
CREATE SINK contacts_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.contacts_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'contacts_v2')
KEY (id, property_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;
```

#### Alternative Solution: Use Sentinel Values
```sql
SELECT
  activity_id,
  COALESCE(manager_id, -1) AS manager_id,
  -- other columns