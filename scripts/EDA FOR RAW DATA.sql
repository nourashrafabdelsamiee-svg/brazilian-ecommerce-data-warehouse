/******************************************************************************************
 * Contains:
 *   1. sp_eda_per_table     → Full column-level EDA for any table
 *   2. sp_check_integrity   → Referential integrity checks
 *
 * Run after loading data into STAGING, before building the warehouse.
 ******************************************************************************************/

USE [Brazilian E-Commerce Data Warehouse];
GO

PRINT '================================================================';
PRINT 'Creating EDA & Data Quality Procedures';
PRINT '================================================================';
GO

-- =============================================
-- 1. DROP & CREATE sp_eda_per_table (100% WORKING VERSION)
-- =============================================
IF OBJECT_ID('STAGING.sp_eda_per_table', 'P') IS NOT NULL
    DROP PROCEDURE STAGING.sp_eda_per_table;
GO

CREATE PROCEDURE STAGING.sp_eda_per_table
    @table_name NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '→ Running EDA for table: ' + @table_name;

    -- Temp table to hold results
    IF OBJECT_ID('tempdb..#eda') IS NOT NULL DROP TABLE #eda;

    CREATE TABLE #eda (
        table_name     NVARCHAR(128),
        column_name    NVARCHAR(128),
        total_rows     BIGINT,
        distinct_cnt   BIGINT,
        null_cnt       BIGINT,
        min_val        NVARCHAR(255),
        max_val        NVARCHAR(255),
        avg_val        FLOAT,
        avg_length     FLOAT,
        negative_cnt   BIGINT,
        invalid_date   BIGINT
    );

    DECLARE @col NVARCHAR(128), @type INT, @sql NVARCHAR(MAX);

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT name, system_type_id
        FROM sys.columns
        WHERE object_id = OBJECT_ID('STAGING.' + QUOTENAME(@table_name));

    OPEN cur;
    FETCH NEXT FROM cur INTO @col, @type;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- DATETIME columns
        IF @type IN (40,41,42,61) -- date, datetime, datetime2, smalldatetime
        BEGIN
            SET @sql = '
            INSERT INTO #eda (table_name, column_name, total_rows, distinct_cnt, null_cnt, min_val, max_val)
            SELECT ''' + @table_name + ''', ''' + @col + ''',
                   COUNT(*),
                   COUNT(DISTINCT [' + @col + ']),
                   SUM(CASE WHEN [' + @col + '] IS NULL THEN 1 ELSE 0 END),
                   CONVERT(NVARCHAR(23), MIN([' + @col + ']), 120),
                   CONVERT(NVARCHAR(23), MAX([' + @col + ']), 120)
            FROM STAGING.' + QUOTENAME(@table_name);
        END
        -- VARCHAR dates/timestamps
        ELSE IF @type IN (167,231) AND (@col LIKE '%date%' OR @col LIKE '%timestamp%')
        BEGIN
            SET @sql = '
            INSERT INTO #eda (table_name, column_name, total_rows, distinct_cnt, null_cnt, min_val, max_val, avg_length, invalid_date)
            SELECT ''' + @table_name + ''', ''' + @col + ''',
                   COUNT(*),
                   COUNT(DISTINCT [' + @col + ']),
                   SUM(CASE WHEN [' + @col + '] IS NULL THEN 1 ELSE 0 END),
                   MIN([' + @col + ']),
                   MAX([' + @col + ']),
                   AVG(LEN([' + @col + '])),
                   SUM(CASE WHEN ISDATE([' + @col + ']) = 0 AND [' + @col + '] IS NOT NULL THEN 1 ELSE 0 END)
            FROM STAGING.' + QUOTENAME(@table_name);
        END
        -- String columns
        ELSE IF @type IN (167,175,231,239) -- varchar, char, nvarchar, nchar
        BEGIN
            SET @sql = '
            INSERT INTO #eda (table_name, column_name, total_rows, distinct_cnt, null_cnt, avg_length)
            SELECT ''' + @table_name + ''', ''' + @col + ''',
                   COUNT(*),
                   COUNT(DISTINCT [' + @col + ']),
                   SUM(CASE WHEN [' + @col + '] IS NULL THEN 1 ELSE 0 END),
                   AVG(LEN([' + @col + ']))
            FROM STAGING.' + QUOTENAME(@table_name);
        END
        -- Numeric columns
        ELSE IF @type IN (52,56,62,127,108,106) -- int, bigint, float, decimal, numeric, etc.
        BEGIN
            SET @sql = '
            INSERT INTO #eda (table_name, column_name, total_rows, distinct_cnt, null_cnt, min_val, max_val, avg_val, negative_cnt)
            SELECT ''' + @table_name + ''', ''' + @col + ''',
                   COUNT(*),
                   COUNT(DISTINCT [' + @col + ']),
                   SUM(CASE WHEN [' + @col + '] IS NULL THEN 1 ELSE 0 END),
                   CONVERT(NVARCHAR(50), MIN([' + @col + '])),
                   CONVERT(NVARCHAR(50), MAX([' + @col + '])),
                   AVG(CAST([' + @col + '] AS FLOAT)),
                   SUM(CASE WHEN [' + @col + '] < 0 THEN 1 ELSE 0 END)
            FROM STAGING.' + QUOTENAME(@table_name);
        END
        -- Other types (just basic stats)
        ELSE
        BEGIN
            SET @sql = '
            INSERT INTO #eda (table_name, column_name, total_rows, distinct_cnt, null_cnt)
            SELECT ''' + @table_name + ''', ''' + @col + ''',
                   COUNT(*),
                   COUNT(DISTINCT [' + @col + ']),
                   SUM(CASE WHEN [' + @col + '] IS NULL THEN 1 ELSE 0 END)
            FROM STAGING.' + QUOTENAME(@table_name);
        END

        EXEC(@sql);

        FETCH NEXT FROM cur INTO @col, @type;
    END

    CLOSE cur;
    DEALLOCATE cur;

    -- Return results
    SELECT 
        table_name,
        column_name,
        total_rows,
        distinct_cnt,
        null_cnt,
        min_val,
        max_val,
        ISNULL(CAST(avg_val AS NVARCHAR), '-') AS avg_val,
        ISNULL(CAST(avg_length AS NVARCHAR), '-') AS avg_length,
        ISNULL(CAST(negative_cnt AS NVARCHAR), '-') AS negative_cnt,
        ISNULL(CAST(invalid_date AS NVARCHAR), '-') AS invalid_date
    FROM #eda
    ORDER BY column_name;

    PRINT 'EDA completed for ' + @table_name;
END
GO

PRINT 'sp_eda_per_table created successfully.';
GO

-- =============================================
-- 2. sp_check_integrity (clean & working)
-- =============================================
IF OBJECT_ID('STAGING.sp_check_integrity', 'P') IS NOT NULL
    DROP PROCEDURE STAGING.sp_check_integrity;
GO

CREATE PROCEDURE STAGING.sp_check_integrity
AS
BEGIN
    SET NOCOUNT ON;

    PRINT 'Starting referential integrity checks...';

    PRINT '--- Orphan Payments ---';
    SELECT p.order_id, COUNT(*) AS cnt
    FROM STAGING.payments p
    LEFT JOIN STAGING.orders o ON p.order_id = o.order_id
    WHERE o.order_id IS NULL
    GROUP BY p.order_id;

    PRINT '--- Orphan Order Items ---';
    SELECT oi.order_id, COUNT(*) AS cnt
    FROM STAGING.order_items oi
    LEFT JOIN STAGING.orders o ON oi.order_id = o.order_id
    WHERE o.order_id IS NULL
    GROUP BY oi.order_id;

    PRINT '--- Orphan Reviews ---';
    SELECT r.order_id, COUNT(*) AS cnt
    FROM STAGING.reviews r
    LEFT JOIN STAGING.orders o ON r.order_id = o.order_id
    WHERE o.order_id IS NULL
    GROUP BY r.order_id;

    PRINT '--- Inactive Sellers (no sales) ---';
    SELECT COUNT(*) AS inactive_sellers
    FROM STAGING.sellers s
    LEFT JOIN STAGING.order_items oi ON s.seller_id = oi.seller_id
    WHERE oi.seller_id IS NULL;

    PRINT '--- Unsold Products ---';
    SELECT COUNT(*) AS unsold_products
    FROM STAGING.products p
    LEFT JOIN STAGING.order_items oi ON p.product_id = oi.product_id
    WHERE oi.product_id IS NULL;

    PRINT '--- Missing Customer Zip Codes ---';
    SELECT COUNT(*) AS missing_zip_count
    FROM STAGING.customers
    WHERE customer_zip_code_prefix IS NULL;

    PRINT '--- Customers with no orders (should be 0) ---';
    SELECT COUNT(*) AS orphan_customers
    FROM STAGING.customers c
    LEFT JOIN STAGING.orders o ON c.customer_id = o.customer_id
    WHERE o.customer_id IS NULL;

    PRINT 'All integrity checks completed!';
END
GO

PRINT 'sp_check_integrity created successfully.';
GO

-- =============================================
-- Optional: Run on all tables
-- =============================================
PRINT 'Running EDA on main tables...';
GO
EXEC STAGING.sp_eda_per_table 'orders';
EXEC STAGING.sp_eda_per_table 'order_items';
EXEC STAGING.sp_eda_per_table 'customers';
EXEC STAGING.sp_eda_per_table 'products';
EXEC STAGING.sp_eda_per_table 'payments';
EXEC STAGING.sp_eda_per_table 'reviews';
EXEC STAGING.sp_eda_per_table 'sellers';
EXEC STAGING.sp_eda_per_table 'category_translation';
GO

EXEC STAGING.sp_check_integrity;
GO

PRINT '================================================================';
PRINT 'EDA Procedures script finished successfully!';
PRINT '================================================================';
GO