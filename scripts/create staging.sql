/******************************************************************************************
  * This script should be run FIRST to set up the staging area.
 * All tables store data exactly as it comes from the CSV files (no transformation yet).
 ******************************************************************************************/

USE [Brazilian E-Commerce Data Warehouse];
GO

-- =============================================
-- 1. Create STAGING schema (if not exists)
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'STAGING')
BEGIN
    EXEC('CREATE SCHEMA STAGING');
    PRINT 'STAGING schema created successfully.';
END
ELSE
    PRINT 'STAGING schema already exists.';
GO

-- =============================================
-- 2. Drop existing tables if they exist (for re-runs)
-- =============================================
DROP TABLE IF EXISTS STAGING.geolocation;
DROP TABLE IF EXISTS STAGING.category_translation;
DROP TABLE IF EXISTS STAGING.reviews;
DROP TABLE IF EXISTS STAGING.payments;
DROP TABLE IF EXISTS STAGING.sellers;
DROP TABLE IF EXISTS STAGING.products;
DROP TABLE IF EXISTS STAGING.customers;
DROP TABLE IF EXISTS STAGING.order_items;
DROP TABLE IF EXISTS STAGING.orders;
GO

-- =============================================
-- 3. Create raw staging tables (one per CSV file)
-- =============================================

-- Orders table - contains order metadata and timestamps (stored as VARCHAR for raw load)
CREATE TABLE STAGING.orders (
    order_id                        VARCHAR(50)     NOT NULL,
    customer_id                     VARCHAR(50)     NOT NULL,
    order_status                    VARCHAR(20)     NOT NULL,
    order_purchase_timestamp        VARCHAR(50),
    order_approved_at               VARCHAR(50),
    order_delivered_carrier_date    VARCHAR(50),
    order_delivered_customer_date   VARCHAR(50),
    order_estimated_delivery_date   VARCHAR(50)
);
PRINT 'Table STAGING.orders created.';
GO

-- Order items - line level detail (one row per product in order)
CREATE TABLE STAGING.order_items (
    order_id            VARCHAR(50)     NOT NULL,
    order_item_id       INT             NOT NULL,
    product_id          VARCHAR(50)     NOT NULL,
    seller_id           VARCHAR(50)     NOT NULL,
    shipping_limit_date VARCHAR(50)     NOT NULL,
    price               DECIMAL(10,2)   NOT NULL,
    freight_value       DECIMAL(10,2)   NOT NULL
);
PRINT 'Table STAGING.order_items created.';
GO

-- Customers - one row per order (customer_id is session-based, not unique person)
CREATE TABLE STAGING.customers (
    customer_id             VARCHAR(50)     NOT NULL,
    customer_unique_id      VARCHAR(50)     NOT NULL,  -- actual unique customer
    customer_zip_code_prefix VARCHAR(10),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(2)
);
PRINT 'Table STAGING.customers created.';
GO

-- Products - product master data
CREATE TABLE STAGING.products (
    product_id                  VARCHAR(50) NOT NULL,
    product_category_name       VARCHAR(100),
    product_name_length         INT,
    product_description_length  INT,
    product_photos_qty          INT,
    product_weight_g            INT,
    product_length_cm           INT,
    product_height_cm           INT,
    product_width_cm            INT
);
PRINT 'Table STAGING.products created.';
GO

-- Sellers - seller master data
CREATE TABLE STAGING.sellers (
    seller_id                   VARCHAR(50) NOT NULL,
    seller_zip_code_prefix      VARCHAR(10),
    seller_city                 VARCHAR(100),
    seller_state                VARCHAR(2)
);
PRINT 'Table STAGING.sellers created.';
GO

-- Payments - multiple payments possible per order
CREATE TABLE STAGING.payments (
    order_id                VARCHAR(50) NOT NULL,
    payment_sequential      INT         NOT NULL,
    payment_type            VARCHAR(50),
    payment_installments    INT,
    payment_value           DECIMAL(10,2)
);
PRINT 'Table STAGING.payments created.';
GO

-- Reviews - one review per order (sometimes multiple, but rare)
CREATE TABLE STAGING.reviews (
    review_id                   VARCHAR(50) NOT NULL,
    order_id                    VARCHAR(50) NOT NULL,
    review_score                INT,
    review_comment_title        TEXT,
    review_comment_message      TEXT,
    review_creation_date        VARCHAR(50),
    review_answer_timestamp     VARCHAR(50)
);
PRINT 'Table STAGING.reviews created.';
GO

-- Category translation - Portuguese to English
CREATE TABLE STAGING.category_translation (
    product_category_name           VARCHAR(100) NOT NULL,
    product_category_name_english   VARCHAR(100)
);
PRINT 'Table STAGING.category_translation created.';
GO

-- Geolocation - zip code to lat/lng mapping (not used in final model but kept for reference)
CREATE TABLE STAGING.geolocation (
    geolocation_zip_code_prefix VARCHAR(10)     NOT NULL,
    geolocation_lat             DECIMAL(10,7),
    geolocation_lng             DECIMAL(10,7),
    geolocation_city            VARCHAR(100),
    geolocation_state           VARCHAR(2)
);
PRINT 'Table STAGING.geolocation created.';
GO

-- =============================================
-- 4. Create WAREHOUSE schema (for final star schema)
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'WAREHOUSE')
BEGIN
    EXEC('CREATE SCHEMA WAREHOUSE');
    PRINT 'WAREHOUSE schema created successfully.';
END
ELSE
    PRINT 'WAREHOUSE schema already exists.';
GO

-- =============================================
-- 5. Final check - list all staging tables
-- =============================================
PRINT '--- STAGING tables created ---';
SELECT 
    TABLE_NAME AS staging_table
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'STAGING'
ORDER BY TABLE_NAME;
GO

PRINT '================================================================';
PRINT 'create_staging.sql executed successfully!';
PRINT 'Next step: Run load_staging.py to load CSV files into STAGING';
PRINT '================================================================';
GO