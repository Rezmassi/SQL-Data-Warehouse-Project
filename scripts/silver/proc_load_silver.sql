/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    total_start_time TIMESTAMP;
    total_end_time TIMESTAMP;
    total_duration INTERVAL;
BEGIN
    -- Capture start time for the entire Silver layer load
    total_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Started loading Silver layer at %', total_start_time;

    -- =======================================================================
    -- 1. TRUNCATE Silver Tables
    -- =======================================================================
    RAISE NOTICE 'Truncating Silver tables...';
    TRUNCATE TABLE silver.crm_cust_info;
    TRUNCATE TABLE silver.crm_sales_details;
    TRUNCATE TABLE silver.crm_prd_info;
    TRUNCATE TABLE silver.erp_CUST_AZ12;
    TRUNCATE TABLE silver.erp_LOC_A101;
    TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
    RAISE NOTICE 'Truncation complete.';
    
    -- =======================================================================
    -- 2. LOAD silver.crm_cust_info
    -- =======================================================================
    RAISE NOTICE 'Loading silver.crm_cust_info...';
    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname, 
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT 
            *, 
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM 
            bronze.crm_cust_info
    ) t 
    WHERE flag_last = 1;
    RAISE NOTICE 'silver.crm_cust_info loaded.';

    -- =======================================================================
    -- 3. LOAD silver.crm_prd_info
    -- =======================================================================
    RAISE NOTICE 'Loading silver.crm_prd_info...';
    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost, 
        prd_line, prd_start_dt, prd_end_dt
    )
    SELECT 
        prd_id, 
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, 
        prd_nm, 
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line)) 
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE
        ) AS prd_end_dt
    FROM 
        bronze.crm_prd_info;
    RAISE NOTICE 'silver.crm_prd_info loaded.';


    -- =======================================================================
    -- 4. Loading silver.crm_sales_details

	--- Since the Bronze layer columns are all TEXT,

	---and the Silver layer columns are INTEGER, you must perform two steps simultaneously for sls_sales, sls_quantity, and sls_price:
	---with a CTE (cleaned_sales)
    -- =======================================================================

    RAISE NOTICE 'Loading silver.crm_sales_details...';
    WITH cleaned_sales AS (
        -- STEP 1: Preliminary Cleaning and Type Casting
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            -- Clean and convert Sales, Quantity, Price to temporary INTEGERs
            NULLIF(TRIM(sls_sales), '0')::INTEGER AS temp_sls_sales,
            NULLIF(TRIM(sls_quantity), '0')::INTEGER AS temp_sls_quantity,
            NULLIF(TRIM(sls_price), '0')::INTEGER AS temp_sls_price
        FROM
            bronze.crm_sales_details
    )
    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, 
        sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT
        cs.sls_ord_num,
        cs.sls_prd_key,
        NULLIF(TRIM(cs.sls_cust_id), '0')::INTEGER AS sls_cust_id,
        
        -- converting strings to date
        CASE 
            WHEN TRIM(cs.sls_order_dt) = '0' OR LENGTH(TRIM(cs.sls_order_dt)) != 8 THEN NULL
            ELSE TO_DATE(TRIM(cs.sls_order_dt), 'YYYYMMDD') 
        END AS sls_order_dt,
        
        CASE 
            WHEN TRIM(cs.sls_ship_dt) = '0' OR LENGTH(TRIM(cs.sls_ship_dt)) != 8 THEN NULL
            ELSE TO_DATE(TRIM(cs.sls_ship_dt), 'YYYYMMDD') 
        END AS sls_ship_dt,
        
        CASE 
            WHEN TRIM(cs.sls_due_dt) = '0' OR LENGTH(TRIM(cs.sls_due_dt)) != 8 THEN NULL
            ELSE TO_DATE(TRIM(cs.sls_due_dt), 'YYYYMMDD') 
        END AS sls_due_dt,

        -- Calculated Price (P)
        CASE 
            WHEN cs.temp_sls_price IS NULL OR cs.temp_sls_price <= 0 
                THEN COALESCE(cs.temp_sls_sales / NULLIF(cs.temp_sls_quantity, 0), 0)
            ELSE ABS(cs.temp_sls_price) 
        END AS sls_price,
        
        -- Calculated Quantity (Q)
        CASE
            WHEN cs.temp_sls_quantity IS NULL OR cs.temp_sls_quantity <= 0 THEN 
                COALESCE(cs.temp_sls_sales / NULLIF(ABS(cs.temp_sls_price), 0), 1)
            ELSE cs.temp_sls_quantity
        END AS sls_quantity,
        
        -- Calculated Sales (S)
        CASE 
            WHEN cs.temp_sls_sales IS NULL 
                 OR cs.temp_sls_sales <= 0 
                 OR cs.temp_sls_sales != cs.temp_sls_quantity * ABS(cs.temp_sls_price)
                THEN cs.temp_sls_quantity * ABS(cs.temp_sls_price)
            ELSE cs.temp_sls_sales
        END AS sls_sales
    FROM
        cleaned_sales cs; 
    RAISE NOTICE 'silver.crm_sales_details loaded.';

    -- =======================================================================
    -- 5. LOAD silver.erp_cust_az12
    -- =======================================================================
    RAISE NOTICE 'Loading silver.erp_cust_az12...';
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT  
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid) - 3) -- Adjusted SUBSTRING length
            ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > CURRENT_DATE THEN NULL -- Changed CURRENT_TIMESTAMP to CURRENT_DATE for comparison
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM 
        bronze.erp_cust_az12;
    RAISE NOTICE 'silver.erp_cust_az12 loaded.';

    -- =======================================================================
    -- 6. LOAD silver.erp_loc_a101
    -- =======================================================================
    RAISE NOTICE 'Loading silver.erp_loc_a101...';
    INSERT INTO silver.erp_loc_a101(cid, cntry)
    SELECT 
        REPLACE (cid, '-', ''),
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = 'UK' THEN 'United Kingdom'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM 
        bronze.erp_loc_a101;
    RAISE NOTICE 'silver.erp_loc_a101 loaded.';

    -- =======================================================================
    -- 7. LOAD silver.erp_px_cat_g1v2
    -- =======================================================================
    RAISE NOTICE 'Loading silver.erp_px_cat_g1v2...';
    INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
    SELECT
        id, 
        cat,
        subcat,
        maintenance
    FROM 
        bronze.erp_px_cat_g1v2;
    RAISE NOTICE 'silver.erp_px_cat_g1v2 loaded.';
    
    -- =======================================================================
    -- Finalization
    -- =======================================================================---

    total_end_time := CURRENT_TIMESTAMP;
    total_duration := total_end_time - total_start_time;
    RAISE NOTICE 'Successfully completed loading Silver layer.';
    RAISE NOTICE 'Total duration: %', total_duration;

END;
$$;


call silver.load_silver();