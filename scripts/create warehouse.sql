/******************************************************************************************

 * This is the CORE of the project: creates clean dimensions and a single, powerful fact table.
 * Run this script AFTER:
 *   1. create_staging.sql
 *   2. load_staging.py (CSV → STAGING)
 *   3. (Optional) eda_procedures.sql
 ******************************************************************************************/

USE [Brazilian E-Commerce Data Warehouse];
GO

PRINT '================================================================';
PRINT 'Starting creation of WAREHOUSE schema (Star Schema)';
PRINT '================================================================';
GO

-- =============================================
-- 1. Drop existing dimension tables (for re-runs)
-- =============================================
DROP TABLE IF EXISTS WAREHOUSE.dim_date;
DROP TABLE IF EXISTS WAREHOUSE.dim_customer;
DROP TABLE IF EXISTS WAREHOUSE.dim_seller;
DROP TABLE IF EXISTS WAREHOUSE.dim_product;
DROP TABLE IF EXISTS WAREHOUSE.fact_sales;
GO

-- =============================================
-- 2. dim_date - Calendar table (2016 to 2020)
-- =============================================
PRINT 'Creating dim_date...';
WITH DateSeq AS (
    SELECT CAST('2016-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM DateSeq
    WHERE dt < '2021-01-01'
)
SELECT
    CAST(FORMAT(dt, 'yyyyMMdd') AS INT) AS date_key,
    dt AS full_date
INTO WAREHOUSE.dim_date
FROM DateSeq
OPTION (MAXRECURSION 0);

ALTER TABLE WAREHOUSE.dim_date ALTER COLUMN date_key INT NOT NULL;
ALTER TABLE WAREHOUSE.dim_date ADD CONSTRAINT PK_dim_date PRIMARY KEY (date_key);
PRINT '→ dim_date created successfully.';
GO

-- =============================================
-- 3. dim_customer - One row per unique customer (using customer_unique_id)
-- =============================================
PRINT 'Creating dim_customer...';
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_unique_id, customer_id) AS customer_sk,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    UPPER(LTRIM(RTRIM(customer_city))) AS customer_city,
    customer_state
INTO WAREHOUSE.dim_customer
FROM STAGING.customers;

ALTER TABLE WAREHOUSE.dim_customer ALTER COLUMN customer_sk BIGINT NOT NULL;
ALTER TABLE WAREHOUSE.dim_customer ADD PRIMARY KEY (customer_sk);
PRINT '→ dim_customer created successfully.';
GO

-- =============================================
-- 4. dim_seller - Seller master data
-- =============================================
PRINT 'Creating dim_seller...';
SELECT
    ROW_NUMBER() OVER (ORDER BY seller_id) AS seller_sk,
    seller_id,
    seller_zip_code_prefix,
    UPPER(LTRIM(RTRIM(seller_city))) AS seller_city,
    seller_state
INTO WAREHOUSE.dim_seller
FROM STAGING.sellers;

ALTER TABLE WAREHOUSE.dim_seller ALTER COLUMN seller_sk BIGINT NOT NULL;
ALTER TABLE WAREHOUSE.dim_seller ADD PRIMARY KEY (seller_sk);
PRINT '→ dim_seller created successfully.';
GO

-- =============================================
-- 5. dim_product - Product master with English category
-- =============================================
PRINT 'Creating dim_product...';
SELECT
    ROW_NUMBER() OVER (ORDER BY p.product_id) AS product_sk,
    p.product_id,
    COALESCE(p.product_category_name, 'Unknown') AS category_pt,
    COALESCE(t.product_category_name_english, 'Unknown') AS category_en,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
INTO WAREHOUSE.dim_product
FROM STAGING.products p
LEFT JOIN STAGING.category_translation t
    ON p.product_category_name = t.product_category_name;

ALTER TABLE WAREHOUSE.dim_product ALTER COLUMN product_sk BIGINT NOT NULL;
ALTER TABLE WAREHOUSE.dim_product ADD PRIMARY KEY (product_sk);
PRINT '→ dim_product created successfully.';
GO

-- =============================================
-- 6. fact_sales - The main fact table (112,650 rows)
-- =============================================
PRINT 'Creating fact_sales table...';
CREATE TABLE WAREHOUSE.fact_sales (
    sales_sk             BIGINT IDENTITY(1,1) PRIMARY KEY,
    order_id             VARCHAR(32)   NOT NULL,
    order_item_id        INT           NOT NULL,
    
    customer_sk          BIGINT        NOT NULL,
    seller_sk            BIGINT        NOT NULL,
    product_sk           BIGINT        NOT NULL,
    
    purchase_date_key    INT           NOT NULL,
    delivered_date_key   INT,
    estimated_date_key   INT           NOT NULL,
    
    price                DECIMAL(12,2) NOT NULL,
    freight_value        DECIMAL(12,2) NOT NULL,
    line_total           AS price + freight_value PERSISTED,
    
    payment_type         VARCHAR(20),
    payment_installments INT,
    payment_value        DECIMAL(12,2),
    
    review_score         TINYINT,
    
    order_status         VARCHAR(20),
    is_delivered         BIT DEFAULT 0,
    is_canceled          BIT DEFAULT 0,
    is_late              BIT DEFAULT 0,
    days_to_deliver      INT,
    days_late            INT
);

-- Foreign Keys
ALTER TABLE WAREHOUSE.fact_sales ADD CONSTRAINT FK_sales_customer FOREIGN KEY (customer_sk)  REFERENCES WAREHOUSE.dim_customer(customer_sk);
ALTER TABLE WAREHOUSE.fact_sales ADD CONSTRAINT FK_sales_seller   FOREIGN KEY (seller_sk)    REFERENCES WAREHOUSE.dim_seller(seller_sk);
ALTER TABLE WAREHOUSE.fact_sales ADD CONSTRAINT FK_sales_product  FOREIGN KEY (product_sk)   REFERENCES WAREHOUSE.dim_product(product_sk);
ALTER TABLE WAREHOUSE.fact_sales ADD CONSTRAINT FK_sales_date     FOREIGN KEY (purchase_date_key) REFERENCES WAREHOUSE.dim_date(date_key);

PRINT '→ fact_sales table created with constraints.';
GO

-- =============================================
-- 7. Load fact_sales with full transformation
-- =============================================
PRINT 'Loading data into fact_sales (this may take 10-20 seconds)...';
TRUNCATE TABLE WAREHOUSE.fact_sales;
GO

INSERT INTO WAREHOUSE.fact_sales (
    order_id, order_item_id,
    customer_sk, seller_sk, product_sk,
    purchase_date_key, delivered_date_key, estimated_date_key,
    price, freight_value,
    payment_type, payment_installments, payment_value,
    review_score,
    order_status,
    is_delivered, is_canceled, is_late,
    days_to_deliver, days_late
)
SELECT
    oi.order_id,
    oi.order_item_id,

    c.customer_sk,
    s.seller_sk,
    p.product_sk,

    -- Safe date conversion (handles VARCHAR dates from CSV)
    CAST(FORMAT(TRY_CONVERT(DATETIME, o.order_purchase_timestamp), 'yyyyMMdd') AS INT) AS purchase_date_key,
    
    CASE WHEN TRY_CONVERT(DATETIME, o.order_delivered_customer_date) IS NOT NULL
         THEN CAST(FORMAT(TRY_CONVERT(DATETIME, o.order_delivered_customer_date), 'yyyyMMdd') AS INT)
         ELSE NULL END AS delivered_date_key,
    
    CAST(FORMAT(TRY_CONVERT(DATETIME, o.order_estimated_delivery_date), 'yyyyMMdd') AS INT) AS estimated_date_key,

    oi.price,
    oi.freight_value,

    -- Payment: First payment type and total value
    pay.payment_type,
    pay.payment_installments,
    pay.total_payment_value,

    -- Review: Average score per order
    COALESCE(rev.avg_review_score, 0) AS review_score,

    o.order_status,

    CASE WHEN o.order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END AS is_delivered,
    CASE WHEN o.order_status IN ('canceled', 'unavailable') THEN 1 ELSE 0 END AS is_canceled,
    CASE WHEN TRY_CONVERT(DATETIME, o.order_delivered_customer_date) > TRY_CONVERT(DATETIME, o.order_estimated_delivery_date) THEN 1 ELSE 0 END AS is_late,

    DATEDIFF(DAY, TRY_CONVERT(DATETIME, o.order_purchase_timestamp), TRY_CONVERT(DATETIME, o.order_delivered_customer_date)) AS days_to_deliver,
    DATEDIFF(DAY, TRY_CONVERT(DATETIME, o.order_estimated_delivery_date), TRY_CONVERT(DATETIME, o.order_delivered_customer_date)) AS days_late

FROM STAGING.order_items oi
JOIN STAGING.orders o ON oi.order_id = o.order_id
JOIN WAREHOUSE.dim_customer c ON o.customer_id = c.customer_id
JOIN WAREHOUSE.dim_seller s ON oi.seller_id = s.seller_id
JOIN WAREHOUSE.dim_product p ON oi.product_id = p.product_id

-- First payment per order (more accurate than MAX)
LEFT JOIN (
    SELECT order_id,
           FIRST_VALUE(payment_type) OVER (PARTITION BY order_id ORDER BY payment_sequential) AS payment_type,
           FIRST_VALUE(payment_installments) OVER (PARTITION BY order_id ORDER BY payment_sequential) AS payment_installments,
           SUM(payment_value) AS total_payment_value
    FROM STAGING.payments
    GROUP BY order_id
) pay ON oi.order_id = pay.order_id

-- Average review score per order
LEFT JOIN (
    SELECT order_id,
           ROUND(AVG(CAST(review_score AS FLOAT)), 1) AS avg_review_score
    FROM STAGING.reviews
    GROUP BY order_id
) rev ON oi.order_id = rev.order_id;
GO

-- =============================================
-- Final result
-- =============================================
DECLARE @row_count INT = (SELECT COUNT(*) FROM WAREHOUSE.fact_sales);
PRINT '================================================================';
PRINT 'WAREHOUSE BUILD COMPLETE!';
PRINT '→ fact_sales loaded with ' + CAST(@row_count AS VARCHAR) + ' rows (should be ~112,650)';
PRINT '→ Star Schema is ready for analytics!';
PRINT '================================================================';
GO