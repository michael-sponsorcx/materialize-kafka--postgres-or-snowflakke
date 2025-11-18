-- ============================================================
-- MATERIALIZE → KAFKA → SNOWFLAKE PIPELINE SETUP
-- Generated for all views in materialize.public_dbt schema
-- ============================================================

-- PREREQUISITES:
-- 1. Kafka connection exists: materialize.public.kafka_connection
-- 2. Schema Registry connection exists: materialize.public.csr_connection
-- 3. Target cluster exists: quickstart

-- ============================================================
-- STEP 1: CREATE MATERIALIZED VIEWS WITH PRECISION CASTING
-- ============================================================

-- activities
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.activities_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.activities_mv AS
SELECT
  activity_id,
  property_id,
  org_id,
  agreement_id,
  account_id,
  manager_id,
  activity_name,
  date,
  activity_priority,
  activity_completed,
  notes,
  activity_type,
  account_name,
  contact_name,
  manager_name,
  org_name,
  created_at,
  updated_at
FROM materialize.public_dbt.activities;

-- activities_by_manager
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
FROM materialize.public_dbt.activities_by_manager;

-- agreement_revenue
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.agreement_revenue_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.agreement_revenue_mv AS
SELECT
  unique_key,
  agreement_id,
  org_id,
  account_id,
  property_id,
  fiscal_year_id,
  has_asset,
  account_manager_id,
  secondary_account_manager_id,
  last_completed_activity_id,
  unit_total::NUMERIC(18,2) AS unit_total,
  agency_fee::NUMERIC(18,2) AS agency_fee,
  trade_value::NUMERIC(18,2) AS trade_value,
  cash_value::NUMERIC(18,2) AS cash_value,
  hard_costs::NUMERIC(18,2) AS hard_costs,
  revenue::NUMERIC(18,2) AS revenue,
  gross_revenue::NUMERIC(18,2) AS gross_revenue,
  rate_card_total::NUMERIC(18,2) AS rate_card_total,
  cash_amount::NUMERIC(18,2) AS cash_amount,
  budget_relief_amount::NUMERIC(18,2) AS budget_relief_amount,
  expense_offset_barter::NUMERIC(18,2) AS expense_offset_barter,
  campus_cash::NUMERIC(18,2) AS campus_cash,
  campus_budget_relief::NUMERIC(18,2) AS campus_budget_relief,
  campus_expense_offset_barter::NUMERIC(18,2) AS campus_expense_offset_barter,
  non_commissioned_barter_amount::NUMERIC(18,2) AS non_commissioned_barter_amount,
  nil::NUMERIC(18,2) AS nil,
  unallocated_project_expenses_carve_out::NUMERIC(18,2) AS unallocated_project_expenses_carve_out
FROM materialize.public_dbt.agreement_revenue;

-- contacts
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.contacts_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.contacts_mv AS
SELECT
  id,
  account_id,
  org_id,
  property_id,
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
FROM materialize.public_dbt.contacts;

-- dim_accounts
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_accounts_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_accounts_mv AS
SELECT
  id,
  name,
  country,
  notes,
  city,
  archived,
  relationship_type,
  logo,
  street1,
  street2,
  state,
  bc_customer_no,
  zip,
  custom_fields,
  logo_aspect_ratio::NUMERIC(18,6) AS logo_aspect_ratio,
  account_category,
  account_subcategory,
  delivery_region,
  custom_field_description
FROM materialize.public_dbt.dim_accounts;

-- dim_activities
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_activities_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_activities_mv AS
SELECT
  id,
  name,
  created_at,
  updated_at,
  completed,
  priority,
  archived,
  date_of_activity,
  activity_type_label
FROM materialize.public_dbt.dim_activities;

-- dim_agreement_inventories
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_agreement_inventories_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_agreement_inventories_mv AS
SELECT
  id,
  fiscal_year_id,
  title,
  description,
  updated_at,
  rate_card::NUMERIC(18,2) AS rate_card,
  asset_name,
  package_name,
  asset_type,
  asset_category,
  status,
  draft,
  archived,
  gl_code,
  status_notes,
  asset_remarks,
  unit_of_sale_description,
  operational_asset,
  mlb_gl_codes
FROM materialize.public_dbt.dim_agreement_inventories;

-- dim_agreement_stage_change
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_agreement_stage_change_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_agreement_stage_change_mv AS
SELECT
  stage_id,
  previous_stage,
  stage,
  previous_stage_name,
  stage_name,
  stage_change_date,
  next_stage_change_date
FROM materialize.public_dbt.dim_agreement_stage_change;

-- dim_agreements
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_agreements_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_agreements_mv AS
SELECT
  id,
  name,
  agreement_business_type,
  agreement_number,
  end_date,
  description,
  percent_closed_step,
  archived,
  objection_reason,
  objection,
  percent_closed_label,
  percent_closed_value::NUMERIC(18,6) AS percent_closed_value,
  percent_closed_step_change_stage_from,
  percent_closed_label_change_from,
  number_of_seasons,
  start_season,
  number_of_activities,
  lost,
  lost_on,
  executed_at,
  created_at,
  organization_id,
  salesforce_id,
  most_recent_stage_change_date,
  agreement_id,
  objectives,
  exclusivity,
  contract_type,
  classification,
  next_steps,
  challenges,
  hit_list,
  themes,
  prospect_source
FROM materialize.public_dbt.dim_agreements;

-- dim_billing_records
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_billing_records_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_billing_records_mv AS
SELECT
  id,
  season,
  billing_date,
  due_date,
  invoice_number,
  paid
FROM materialize.public_dbt.dim_billing_records;

-- dim_fiscal_years
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_fiscal_years_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_fiscal_years_mv AS
SELECT
  id,
  start_date,
  end_date,
  label,
  start_month,
  is_salesforce
FROM materialize.public_dbt.dim_fiscal_years;

-- dim_inventory_units
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_inventory_units_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_inventory_units_mv AS
SELECT
  id,
  end_date,
  units::NUMERIC(18,2) AS units
FROM materialize.public_dbt.dim_inventory_units;

-- dim_objective_key_results
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_objective_key_results_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_objective_key_results_mv AS
SELECT
  id,
  name,
  pct_to_target::NUMERIC(18,6) AS pct_to_target,
  target_level
FROM materialize.public_dbt.dim_objective_key_results;

-- dim_objectives
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_objectives_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_objectives_mv AS
SELECT
  id,
  title,
  description,
  category,
  start_date,
  end_date,
  updated_at
FROM materialize.public_dbt.dim_objectives;

-- dim_organization_categories
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_organization_categories_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_organization_categories_mv AS
SELECT
  id,
  title
FROM materialize.public_dbt.dim_organization_categories;

-- dim_organization_types
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_organization_types_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_organization_types_mv AS
SELECT
  id,
  title
FROM materialize.public_dbt.dim_organization_types;

-- dim_organizations
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_organizations_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_organizations_mv AS
SELECT
  id,
  name,
  premium_plus_reporting
FROM materialize.public_dbt.dim_organizations;

-- dim_properties
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_properties_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_properties_mv AS
SELECT
  id,
  name,
  organization_id,
  division,
  business_unit,
  archived
FROM materialize.public_dbt.dim_properties;

-- dim_trade
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_trade_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_trade_mv AS
SELECT
  id,
  label
FROM materialize.public_dbt.dim_trade;

-- dim_trade_collections
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_trade_collections_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_trade_collections_mv AS
SELECT
  id,
  date,
  description,
  type
FROM materialize.public_dbt.dim_trade_collections;

-- dim_users
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.dim_users_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.dim_users_mv AS
SELECT
  id,
  first_name,
  last_name,
  email,
  title,
  archived,
  default_organization_id,
  last_sign_in_at,
  admin_as_string,
  admin,
  created_at,
  updated_at,
  user_id
FROM materialize.public_dbt.dim_users;

-- expiring_agreements
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.expiring_agreements_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.expiring_agreements_mv AS
SELECT
  unique_key,
  agreement_id,
  account_id,
  property_id,
  org_id,
  expiring_fiscal_year,
  cash_amount::NUMERIC(18,2) AS cash_amount,
  budget_relief_amount::NUMERIC(18,2) AS budget_relief_amount,
  nil::NUMERIC(18,2) AS nil,
  non_commissioned_barter_amount::NUMERIC(18,2) AS non_commissioned_barter_amount
FROM materialize.public_dbt.expiring_agreements;

-- fact_agreement_stages
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_agreement_stages_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_agreement_stages_mv AS
SELECT
  stage_id,
  agreement_id,
  user_id,
  account_id,
  property_id,
  org_id,
  time_spent,
  percent_time_spent::NUMERIC(18,6) AS percent_time_spent,
  gross_of_deal::NUMERIC(18,2) AS gross_of_deal
FROM materialize.public_dbt.fact_agreement_stages;

-- fact_agreement_stages_van_wagner
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_agreement_stages_van_wagner_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_agreement_stages_van_wagner_mv AS
SELECT
  stage_id,
  agreement_id,
  user_id,
  org_id,
  property_id,
  account_id,
  fiscal_year_id,
  account_manager_id,
  has_asset,
  stage_change_date,
  next_stage_change_date,
  stage_number,
  time_spent,
  percent_time_spent::NUMERIC(18,6) AS percent_time_spent,
  latest_stage,
  unit_total::NUMERIC(18,2) AS unit_total,
  agency_fee::NUMERIC(18,2) AS agency_fee,
  trade_value::NUMERIC(18,2) AS trade_value,
  cash_value::NUMERIC(18,2) AS cash_value,
  hard_costs::NUMERIC(18,2) AS hard_costs,
  revenue::NUMERIC(18,2) AS revenue,
  gross_revenue::NUMERIC(18,2) AS gross_revenue,
  rate_card_total::NUMERIC(18,2) AS rate_card_total,
  cash_amount::NUMERIC(18,2) AS cash_amount,
  budget_relief_amount::NUMERIC(18,2) AS budget_relief_amount,
  campus_cash::NUMERIC(18,2) AS campus_cash,
  campus_budget_relief::NUMERIC(18,2) AS campus_budget_relief,
  non_commissioned_barter_amount::NUMERIC(18,2) AS non_commissioned_barter_amount,
  nil::NUMERIC(18,2) AS nil,
  unallocated_project_expenses_carve_out::NUMERIC(18,2) AS unallocated_project_expenses_carve_out
FROM materialize.public_dbt.fact_agreement_stages_van_wagner;

-- fact_billing
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_billing_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_billing_mv AS
SELECT
  billing_record_id,
  account_id,
  agreement_id,
  org_id,
  fiscal_year_id,
  property_id,
  amount::NUMERIC(18,2) AS amount,
  collected::NUMERIC(18,2) AS collected
FROM materialize.public_dbt.fact_billing;

-- fact_goal_organization
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_goal_organization_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_goal_organization_mv AS
SELECT
  fiscal_year_id,
  org_id,
  amount::NUMERIC(18,2) AS amount,
  ninety_percent_net_revenue::NUMERIC(18,2) AS ninety_percent_net_revenue,
  net_revenue::NUMERIC(18,2) AS net_revenue
FROM materialize.public_dbt.fact_goal_organization;

-- fact_goal_user
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_goal_user_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_goal_user_mv AS
SELECT
  id,
  fiscal_year_id,
  property_id,
  org_id,
  user_id,
  amount::NUMERIC(18,2) AS amount,
  ninety_percent_net_revenue::NUMERIC(18,2) AS ninety_percent_net_revenue,
  net_revenue::NUMERIC(18,2) AS net_revenue,
  gross_revenue::NUMERIC(18,2) AS gross_revenue,
  gross_new::NUMERIC(18,2) AS gross_new,
  gross_upsell::NUMERIC(18,2) AS gross_upsell,
  gross_existing::NUMERIC(18,2) AS gross_existing
FROM materialize.public_dbt.fact_goal_user;

-- fact_inventory
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_inventory_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_inventory_mv AS
SELECT
  agreement_inventory_id,
  agreement_id,
  account_id,
  property_id,
  fiscal_year_id,
  org_id,
  type_id,
  category_id,
  sold_units::NUMERIC(18,2) AS sold_units,
  proposed_units::NUMERIC(18,2) AS proposed_units,
  agreement_inventory_total_units::NUMERIC(18,2) AS agreement_inventory_total_units,
  total_units::NUMERIC(18,2) AS total_units,
  available_units::NUMERIC(18,2) AS available_units
FROM materialize.public_dbt.fact_inventory;

-- fact_inventory_lifecycle
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_inventory_lifecycle_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_inventory_lifecycle_mv AS
SELECT
  agreement_inventory_id,
  agreement_id,
  account_id,
  property_id,
  org_id,
  fiscal_year_id,
  total_units::NUMERIC(18,2) AS total_units,
  delivered_units,
  scheduled_units::NUMERIC(18,2) AS scheduled_units,
  unscheduled_units::NUMERIC(18,2) AS unscheduled_units,
  not_being_used_units::NUMERIC(18,2) AS not_being_used_units,
  hard_costs::NUMERIC(18,2) AS hard_costs
FROM materialize.public_dbt.fact_inventory_lifecycle;

-- fact_objective_key_results
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_objective_key_results_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_objective_key_results_mv AS
SELECT
  id,
  objective_id,
  property_id,
  agreement_id,
  account_id,
  service_manager_id,
  fiscal_year_id,
  org_id,
  target::NUMERIC(18,6) AS target,
  performance::NUMERIC(18,6) AS performance,
  weight::NUMERIC(18,6) AS weight,
  property_adj_weight::NUMERIC(18,6) AS property_adj_weight,
  pct_to_target::NUMERIC(18,6) AS pct_to_target,
  performance_to_target::NUMERIC(18,6) AS performance_to_target,
  objective_score::NUMERIC(18,6) AS objective_score,
  agreement_score::NUMERIC(18,6) AS agreement_score,
  agreement_cash_value::NUMERIC(18,2) AS agreement_cash_value,
  objective_time_left::NUMERIC(18,2) AS objective_time_left
FROM materialize.public_dbt.fact_objective_key_results;

-- fact_objectives
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_objectives_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_objectives_mv AS
SELECT
  objective_id,
  service_manager_id,
  fiscal_year_id,
  agreement_id,
  account_id,
  property_id,
  org_id,
  weight::NUMERIC(18,6) AS weight
FROM materialize.public_dbt.fact_objectives;

-- fact_property_goal
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_property_goal_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_property_goal_mv AS
SELECT
  fiscal_year_id,
  property_id,
  org_id,
  amount::NUMERIC(18,2) AS amount,
  seventy_five_percent_net_revenue::NUMERIC(18,2) AS seventy_five_percent_net_revenue,
  ninety_percent_net_revenue::NUMERIC(18,2) AS ninety_percent_net_revenue,
  net_revenue::NUMERIC(18,2) AS net_revenue,
  proposed_net_revenue::NUMERIC(18,2) AS proposed_net_revenue
FROM materialize.public_dbt.fact_property_goal;

-- fact_revenue_first_season
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_revenue_first_season_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_revenue_first_season_mv AS
SELECT
  agreement_id,
  org_id,
  account_id,
  has_asset,
  property_id,
  fiscal_year_id,
  account_manager_id,
  secondary_account_manager_id,
  last_completed_activity_id,
  unit_total::NUMERIC(18,2) AS unit_total,
  agency_fee::NUMERIC(18,2) AS agency_fee,
  trade_value::NUMERIC(18,2) AS trade_value,
  cash_value::NUMERIC(18,2) AS cash_value,
  hard_costs::NUMERIC(18,2) AS hard_costs,
  revenue::NUMERIC(18,2) AS revenue,
  gross_revenue::NUMERIC(18,2) AS gross_revenue,
  rate_card_total::NUMERIC(18,2) AS rate_card_total,
  cash_amount::NUMERIC(18,2) AS cash_amount,
  budget_relief_amount::NUMERIC(18,2) AS budget_relief_amount,
  campus_cash::NUMERIC(18,2) AS campus_cash,
  campus_budget_relief::NUMERIC(18,2) AS campus_budget_relief,
  non_commissioned_barter_amount::NUMERIC(18,2) AS non_commissioned_barter_amount,
  nil::NUMERIC(18,2) AS nil,
  unallocated_project_expenses_carve_out::NUMERIC(18,2) AS unallocated_project_expenses_carve_out,
  net_revenue_first_season::NUMERIC(18,2) AS net_revenue_first_season,
  gross_revenue_first_season::NUMERIC(18,2) AS gross_revenue_first_season,
  fiscal_year_id_first_season
FROM materialize.public_dbt.fact_revenue_first_season;

-- fact_trade
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_trade_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_trade_mv AS
SELECT
  trade_id,
  agreement_id,
  fiscal_year_id,
  property_id,
  org_id,
  account_id,
  organization_agreement_values_id,
  label,
  used::NUMERIC(18,2) AS used,
  collected::NUMERIC(18,2) AS collected,
  amount::NUMERIC(18,2) AS amount
FROM materialize.public_dbt.fact_trade;

-- fact_trade_collections
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fact_trade_collections_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fact_trade_collections_mv AS
SELECT
  agreement_trade_collections_id,
  agreement_id,
  oav_id,
  org_id,
  fiscal_year_id,
  organization_agreement_values_id,
  property_id,
  account_id,
  label,
  amount::NUMERIC(18,2) AS amount
FROM materialize.public_dbt.fact_trade_collections;

-- fulfillment_by_type
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.fulfillment_by_type_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.fulfillment_by_type_mv AS
SELECT
  id,
  fiscal_year_id,
  account_id,
  property_id,
  org_id,
  account_name,
  asset_name,
  asset_description,
  fulfillment_type,
  task_name,
  end_date,
  start_date,
  status,
  assignees,
  org_type,
  category,
  units::NUMERIC(18,2) AS units
FROM materialize.public_dbt.fulfillment_by_type;

-- inventory_rate_analysis
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.inventory_rate_analysis_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.inventory_rate_analysis_mv AS
SELECT
  id,
  agreement_inventory_id,
  asset_name,
  package_name,
  type,
  category,
  account_id,
  agreement_id,
  org_id,
  property_id,
  fiscal_year_id,
  units::NUMERIC(18,2) AS units,
  sold_units::NUMERIC(18,2) AS sold_units,
  selling_rate::NUMERIC(18,2) AS selling_rate,
  hard_costs::NUMERIC(18,2) AS hard_costs,
  is_bonus,
  rate_card::NUMERIC(18,2) AS rate_card
FROM materialize.public_dbt.inventory_rate_analysis;

-- permissions_property_and_org_admin
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.permissions_property_and_org_admin_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.permissions_property_and_org_admin_mv AS
SELECT
  property_id,
  org_id,
  authorized_email,
  archived,
  admin
FROM materialize.public_dbt.permissions_property_and_org_admin;

-- permissions_user_and_org_admin
DROP MATERIALIZED VIEW IF EXISTS materialize.public_dbt.permissions_user_and_org_admin_mv CASCADE;
CREATE MATERIALIZED VIEW materialize.public_dbt.permissions_user_and_org_admin_mv AS
SELECT
  user_id,
  org_id,
  authorized_email,
  user_email,
  archived,
  admin
FROM materialize.public_dbt.permissions_user_and_org_admin;

-- ============================================================
-- STEP 2: CREATE KAFKA SINKS
-- ============================================================
-- NOTE: Run these one at a time or in batches to avoid overloading the cluster

-- activities
DROP SINK IF EXISTS activities_sink CASCADE;
CREATE SINK activities_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.activities_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'activities')
KEY (activity_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- activities_by_manager
DROP SINK IF EXISTS activities_by_manager_sink CASCADE;
CREATE SINK activities_by_manager_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.activities_by_manager_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'activities_by_manager')
KEY (activity_id, manager_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- agreement_revenue
DROP SINK IF EXISTS agreement_revenue_sink CASCADE;
CREATE SINK agreement_revenue_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.agreement_revenue_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'agreement_revenue')
KEY (agreement_id, fiscal_year_id, property_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- contacts
DROP SINK IF EXISTS contacts_sink CASCADE;
CREATE SINK contacts_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.contacts_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'contacts')
KEY (id, property_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_accounts
DROP SINK IF EXISTS dim_accounts_sink CASCADE;
CREATE SINK dim_accounts_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_accounts_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_accounts')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_activities
DROP SINK IF EXISTS dim_activities_sink CASCADE;
CREATE SINK dim_activities_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_activities_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_activities')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_agreement_inventories
DROP SINK IF EXISTS dim_agreement_inventories_sink CASCADE;
CREATE SINK dim_agreement_inventories_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_agreement_inventories_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_agreement_inventories')
KEY (id, fiscal_year_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_agreement_stage_change
DROP SINK IF EXISTS dim_agreement_stage_change_sink CASCADE;
CREATE SINK dim_agreement_stage_change_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_agreement_stage_change_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_agreement_stage_change')
KEY (stage_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_agreements
DROP SINK IF EXISTS dim_agreements_sink CASCADE;
CREATE SINK dim_agreements_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_agreements_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_agreements')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_billing_records
DROP SINK IF EXISTS dim_billing_records_sink CASCADE;
CREATE SINK dim_billing_records_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_billing_records_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_billing_records')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_fiscal_years
DROP SINK IF EXISTS dim_fiscal_years_sink CASCADE;
CREATE SINK dim_fiscal_years_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_fiscal_years_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_fiscal_years')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_inventory_units
DROP SINK IF EXISTS dim_inventory_units_sink CASCADE;
CREATE SINK dim_inventory_units_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_inventory_units_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_inventory_units')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_objective_key_results
DROP SINK IF EXISTS dim_objective_key_results_sink CASCADE;
CREATE SINK dim_objective_key_results_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_objective_key_results_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_objective_key_results')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_objectives
DROP SINK IF EXISTS dim_objectives_sink CASCADE;
CREATE SINK dim_objectives_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_objectives_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_objectives')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_organization_categories
DROP SINK IF EXISTS dim_organization_categories_sink CASCADE;
CREATE SINK dim_organization_categories_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_organization_categories_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_organization_categories')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_organization_types
DROP SINK IF EXISTS dim_organization_types_sink CASCADE;
CREATE SINK dim_organization_types_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_organization_types_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_organization_types')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_organizations
DROP SINK IF EXISTS dim_organizations_sink CASCADE;
CREATE SINK dim_organizations_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_organizations_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_organizations')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_properties
DROP SINK IF EXISTS dim_properties_sink CASCADE;
CREATE SINK dim_properties_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_properties_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_properties')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_trade
DROP SINK IF EXISTS dim_trade_sink CASCADE;
CREATE SINK dim_trade_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_trade_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_trade')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_trade_collections
DROP SINK IF EXISTS dim_trade_collections_sink CASCADE;
CREATE SINK dim_trade_collections_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_trade_collections_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_trade_collections')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- dim_users
DROP SINK IF EXISTS dim_users_sink CASCADE;
CREATE SINK dim_users_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.dim_users_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'dim_users')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- expiring_agreements
DROP SINK IF EXISTS expiring_agreements_sink CASCADE;
CREATE SINK expiring_agreements_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.expiring_agreements_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'expiring_agreements')
KEY (agreement_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_agreement_stages
DROP SINK IF EXISTS fact_agreement_stages_sink CASCADE;
CREATE SINK fact_agreement_stages_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_agreement_stages_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_agreement_stages')
KEY (stage_id, property_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_agreement_stages_van_wagner
DROP SINK IF EXISTS fact_agreement_stages_van_wagner_sink CASCADE;
CREATE SINK fact_agreement_stages_van_wagner_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_agreement_stages_van_wagner_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_agreement_stages_van_wagner')
KEY (stage_id, agreement_id, property_id, fiscal_year_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_billing
DROP SINK IF EXISTS fact_billing_sink CASCADE;
CREATE SINK fact_billing_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_billing_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_billing')
KEY (billing_record_id, fiscal_year_id, property_id) NOT ENFORCED  -- Changed from KEY (billing_record_id)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_goal_organization
DROP SINK IF EXISTS fact_goal_organization_sink CASCADE;
CREATE SINK fact_goal_organization_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_goal_organization_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_goal_organization')
KEY (fiscal_year_id, org_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_goal_user
DROP SINK IF EXISTS fact_goal_user_sink CASCADE;
CREATE SINK fact_goal_user_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_goal_user_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_goal_user')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_inventory
DROP SINK IF EXISTS fact_inventory_sink CASCADE;
CREATE SINK fact_inventory_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_inventory_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_inventory')
KEY (agreement_inventory_id, fiscal_year_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_inventory_lifecycle
DROP SINK IF EXISTS fact_inventory_lifecycle_sink CASCADE;
CREATE SINK fact_inventory_lifecycle_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_inventory_lifecycle_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_inventory_lifecycle')
KEY (agreement_inventory_id, fiscal_year_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_objective_key_results
DROP SINK IF EXISTS fact_objective_key_results_sink CASCADE;
CREATE SINK fact_objective_key_results_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_objective_key_results_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_objective_key_results')
KEY (id, property_id) NOT ENFORCED  -- Changed from KEY (id)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_objectives
DROP SINK IF EXISTS fact_objectives_sink CASCADE;
CREATE SINK fact_objectives_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_objectives_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_objectives')
KEY (objective_id, property_id, service_manager_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_property_goal
DROP SINK IF EXISTS fact_property_goal_sink CASCADE;
CREATE SINK fact_property_goal_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_property_goal_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_property_goal')
KEY (fiscal_year_id, property_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_revenue_first_season
DROP SINK IF EXISTS fact_revenue_first_season_sink CASCADE;
CREATE SINK fact_revenue_first_season_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_revenue_first_season_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_revenue_first_season')
KEY (agreement_id, property_id, fiscal_year_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_trade
DROP SINK IF EXISTS fact_trade_sink CASCADE;
CREATE SINK fact_trade_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_trade_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_trade')
KEY (trade_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fact_trade_collections
DROP SINK IF EXISTS fact_trade_collections_sink CASCADE;
CREATE SINK fact_trade_collections_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fact_trade_collections_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fact_trade_collections')
KEY (agreement_trade_collections_id, property_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- fulfillment_by_type
DROP SINK IF EXISTS fulfillment_by_type_sink CASCADE;
CREATE SINK fulfillment_by_type_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.fulfillment_by_type_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'fulfillment_by_type')
KEY (id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- inventory_rate_analysis
DROP SINK IF EXISTS inventory_rate_analysis_sink CASCADE;
CREATE SINK inventory_rate_analysis_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.inventory_rate_analysis_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'inventory_rate_analysis')
KEY (id, fiscal_year_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- permissions_property_and_org_admin
DROP SINK IF EXISTS permissions_property_and_org_admin_sink CASCADE;
CREATE SINK permissions_property_and_org_admin_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.permissions_property_and_org_admin_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'permissions_property_and_org_admin')
KEY (property_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- permissions_user_and_org_admin
DROP SINK IF EXISTS permissions_user_and_org_admin_sink CASCADE;
CREATE SINK permissions_user_and_org_admin_sink
IN CLUSTER quickstart
FROM materialize.public_dbt.permissions_user_and_org_admin_mv
INTO KAFKA CONNECTION materialize.public.kafka_connection (TOPIC = 'permissions_user_and_org_admin')
KEY (user_id, org_id) NOT ENFORCED
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_connection
ENVELOPE UPSERT;

-- ============================================================
-- STEP 3: VERIFY SINKS ARE RUNNING
-- ============================================================

SELECT name, status, error 
FROM mz_internal.mz_sink_statuses 
WHERE name LIKE '%_sink'
ORDER BY name;
