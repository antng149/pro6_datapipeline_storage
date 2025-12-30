-- =============================================================================
-- PROJECT 6: GLAMIRA DATA WAREHOUSE SCHEMA DEFINITION
-- Description: DDL scripts for Bronze Layer tables (Parquet, JSONL, and CSV)
-- =============================================================================

-- 1. DATASET CREATION
CREATE SCHEMA IF NOT EXISTS `glamira_bronze` OPTIONS(location='US');
CREATE SCHEMA IF NOT EXISTS `glamira_project6` OPTIONS(location='US');

-- 2. TABLE: ip_locations
-- Source: CSV (3.2M records)
-- Description: Geographic mapping for IP addresses
CREATE TABLE IF NOT EXISTS `glamira_bronze.ip_locations` (
    ip STRING,
    country_short STRING,
    country_long STRING,
    region STRING,
    city STRING
)
OPTIONS(description="Geographic lookup table for user IP addresses");

-- 3. TABLE: summary_final
-- Source: Parquet (41.4M records, 33 fields)
-- Description: Processed user behavioral events
CREATE TABLE IF NOT EXISTS `glamira_bronze.summary_final` (
    mongo_id STRING,
    event_time TIMESTAMP,
    local_time STRING,
    ip STRING,
    user_id_db STRING,
    device_id STRING,
    email STRING,
    user_agent STRING,
    resolution STRING,
    event_type STRING,
    current_url STRING,
    referrer_url STRING,
    key_search STRING,
    product_id STRING,
    viewing_product_id STRING,
    category_id STRING,
    price FLOAT64,
    currency STRING,
    store_id STRING,
    order_id STRING,
    is_paypal BOOLEAN,
    utm_source STRING,
    utm_medium STRING,
    show_recommendation BOOLEAN,
    recommendation BOOLEAN,
    rec_product_id STRING,
    rec_product_pos STRING,
    rec_clicked_pos STRING,
    options_json STRING,
    cart_products_json STRING
    -- Note: Additional technical metadata fields may exist depending on ingestion
)
PARTITION BY DATE(event_time)
OPTIONS(description="Main behavioral events table with 33 flattened fields");

-- 4. TABLE: products_raw
-- Source: JSONL (18k records, 28 fields)
-- Description: Product metadata and specifications
CREATE TABLE IF NOT EXISTS `glamira_project6.products_raw` (
    sku STRING,
    name STRING,
    url STRING,
    store_code STRING,
    category_name STRING,
    collection STRING,
    collection_id INTEGER,
    product_type STRING,
    product_type_value INTEGER,
    price FLOAT64,
    min_price FLOAT64,
    max_price FLOAT64,
    min_price_format STRING,
    max_price_format STRING,
    gold_weight FLOAT64,
    none_metal_weight FLOAT64,
    material_design STRING,
    gender STRING,
    qty INTEGER,
    type_id STRING,
    attribute_set STRING,
    attribute_set_id INTEGER,
    crawl_status STRING,
    source_domain STRING,
    source_url STRING,
    visible_contents STRING,
    show_popup_quantity_eternity INTEGER,
    bracelet_without_chain INTEGER,
    platinum_palladium_info_in_alloy INTEGER
)
PARTITION BY _PARTITIONDATE
OPTIONS(
    description="Product catalog metadata with 28 detailed jewelry specifications",
    require_partition_filter = false
);