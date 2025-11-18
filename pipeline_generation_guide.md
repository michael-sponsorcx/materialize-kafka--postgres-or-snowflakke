# Materialize → Kafka → Snowflake Pipeline Generation Guide

## Overview
This guide documents the process for automatically generating pipeline setup scripts for streaming data from Materialize views through Kafka to Snowflake tables.

## Prerequisites
- Materialize database with views in a schema (e.g., `materialize.public_dbt`)
- Kafka connection configured in Materialize
- Confluent Schema Registry connection configured in Materialize
- Snowflake account with appropriate permissions
- Claude access for script generation

---

## Step 1: Extract View Schemas from Materialize

Run this query in Materialize to get all column details for your views:

```sql
SELECT 
    v.name AS view_name,
    c.name AS column_name,
    c.type AS data_type,
    c.position
FROM mz_views v
JOIN mz_columns c ON v.id = c.id
WHERE v.schema_id = (
    SELECT id FROM mz_schemas 
    WHERE name = 'public_dbt'  -- Change to your schema name
    AND database_id = (SELECT id FROM mz_databases WHERE name = 'materialize')
)
ORDER BY v.name, c.position;
```

**Export the results** as CSV, table format, or copy the full output.

---

## Step 2: Prepare List of Views to Include

Create a simple list of view names you want to include in the pipeline. This can be:
- A text list (one per line)
- A screenshot of view names
- The list from your data model

Example format:
```
activities
agreement_revenue
contacts
dim_accounts
dim_users
fact_billing
...
```

---

## Step 3: Provide Information to Claude

Give Claude the following inputs in this order:

### 3a. Share the Guide Document
```
I'm setting up a Materialize → Kafka → Snowflake pipeline. Here's the guide document:
[Paste or upload the pipeline guide]
```

### 3b. Share the View List
```
Here are the views I want to include:
[Paste list or upload screenshot]
```

### 3c. Share the Schema Export
```
Here's the schema information from Materialize:
[Paste the query results from Step 1]
```

---

## Step 4: Request Script Generation

Use this prompt template:

```
Please generate the complete pipeline setup for these views:

1. Create materialized views with proper NUMERIC precision casting
2. Create Kafka sinks for each view
3. Provide the Confluent Snowflake connector configuration

Requirements:
- Apply NUMERIC(18,2) for currency/revenue columns
- Apply NUMERIC(18,6) for percentage/ratio columns
- Use appropriate composite keys where needed
- Single Confluent connector to handle all topics
- Include verification queries
```

---

## Step 5: What Claude Will Generate

Claude will create two files:

### File 1: `materialize_pipeline_setup.sql`
Contains:
- DROP and CREATE statements for all materialized views with precision casting
- DROP and CREATE statements for all Kafka sinks
- Verification query to check sink status

### File 2: `confluent_connector_config.txt`
Contains:
- Complete Confluent connector configuration
- Single topic-to-table mapping string for all tables
- Snowflake verification queries

---

## Execution Steps (After Generation)

### 1. In Materialize
```sql
-- Run the generated SQL file
\i materialize_pipeline_setup.sql

-- Or execute in sections if needed
-- Verify all sinks are running
SELECT name, status, error 
FROM mz_internal.mz_sink_statuses 
WHERE name LIKE '%_sink'
ORDER BY name;
```

### 2. In Confluent Cloud
- Navigate to Connectors → Add Connector → Snowflake Sink
- Copy configuration values from `confluent_connector_config.txt`
- Paste the full topic mapping string (all topics in one field)
- Set `value.converter.decimal.format: NUMERIC`
- Create connector

### 3. In Snowflake
```sql
-- Verify tables were created
SHOW TABLES IN SCHEMA ANALYTICS.STREAMING;

-- Check record counts (query provided in generated file)
-- Verify column types (query provided in generated file)
```

---

## Common Customizations

### Different Schema Name
In Step 1, change the schema name in the WHERE clause:
```sql
WHERE name = 'your_schema_name'  -- Instead of 'public_dbt'
```

### Different Precision Requirements
Tell Claude in Step 4:
```
Use NUMERIC(20,4) for all currency fields instead of NUMERIC(18,2)
```

### Different Key Columns
Tell Claude which columns should be composite keys:
```
For agreement_revenue, use composite key: (agreement_id, fiscal_year_id, property_id)
For fact_billing, use single key: (billing_record_id)
```

### Different Cluster
Tell Claude which cluster to use:
```
Use cluster 'production' instead of 'quickstart'
```

---

## Troubleshooting

### "No results" from Schema Query
The views might be tables instead. Try this query:
```sql
SELECT 
    t.name AS table_name,
    c.name AS column_name,
    c.type AS data_type,
    c.position
FROM mz_tables t
JOIN mz_columns c ON t.id = c.id
WHERE t.schema_id = (
    SELECT id FROM mz_schemas 
    WHERE name = 'public_dbt'
    AND database_id = (SELECT id FROM mz_databases WHERE name = 'materialize')
)
ORDER BY t.name, c.position;
```

### Views in Different Schemas
If views are spread across multiple schemas, run the schema query for each schema separately and combine the results.

### Subset of Views Only
Instead of providing all views, just give Claude the list of specific views you want in the pipeline.

---

## Quick Reference Commands

### Find Your Schema Name
```sql
SELECT name 
FROM mz_schemas 
WHERE database_id = (SELECT id FROM mz_databases WHERE name = 'materialize');
```

### Find Your Kafka Connection Name
```sql
SELECT name 
FROM mz_connections 
WHERE type = 'kafka';
```

### Find Your Schema Registry Connection Name
```sql
SELECT name 
FROM mz_connections 
WHERE type = 'csr';
```

### Check Existing Sinks
```sql
SELECT name, type 
FROM mz_sinks;
```

---

## Example Complete Workflow

1. **Extract schema:**
   ```sql
   -- Run extraction query, export to CSV
   ```

2. **Create view list:**
   ```
   agreement_revenue
   contacts
   dim_accounts
   ```

3. **Prompt Claude:**
   ```
   I need to set up a pipeline for these 3 views.
   [Attach guide document]
   [Paste view list]
   [Paste schema export]
   
   Generate the complete setup using:
   - Cluster: production
   - Kafka connection: materialize.public.kafka_connection
   - Schema Registry: materialize.public.csr_connection
   - Snowflake schema: ANALYTICS.STREAMING
   ```

4. **Execute generated scripts** following the execution steps above

---

## Tips for Best Results

1. **Be explicit** about precision requirements for numeric columns
2. **Specify composite keys** if you know which columns should be used
3. **Provide connection names** if they differ from defaults
4. **Mention any constraints** (e.g., "don't create views, just sinks for existing views")
5. **Ask for verification queries** specific to your use case

---

## Maintenance

### Adding New Views
1. Export schema for new views only
2. Provide just the new view names and schemas to Claude
3. Claude generates incremental scripts
4. Execute just the new sink creation statements
5. Update Confluent connector with additional topic mappings

### Modifying Existing Views
When schema changes:
1. Delete Confluent connector
2. Delete schemas in Schema Registry
3. Drop and recreate Materialize sinks
4. Truncate Snowflake tables
5. Recreate Confluent connector

### Removing Deprecated Views
When a view is no longer needed:
1. Remove from Materialize sink (DROP SINK)
2. Remove from connector topic list
3. Optionally delete Kafka topic and schema
4. Optionally drop table in Snowflake

---

## Current Pipeline Status

**Active Tables: 39**

## Files Generated by This Process

- `materialize_pipeline_setup.sql` - Main setup script for Materialize
- `confluent_connector_config.txt` - Configuration for Confluent connector
- `pipeline_generation_guide.md` - This document

Keep these files in version control for reproducibility.
