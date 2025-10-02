/*
===============================================================================
Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the PostgreSQL 'COPY' command to load data from csv Files to bronze tables.
    -Adjusts for Postgres variable declaration.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/



CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    total_start_time TIMESTAMP;
    total_end_time TIMESTAMP;
    total_duration INTERVAL;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
	row_count INTEGER;
BEGIN
    -- Capture start time for the entire Bronze layer
    total_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Started loading Bronze layer at %', total_start_time;

    -- Truncate tables
    TRUNCATE TABLE bronze.crm_cust_info;
    TRUNCATE TABLE bronze.crm_sales_details;
    TRUNCATE TABLE bronze.crm_prd_info;
    TRUNCATE TABLE bronze.erp_CUST_AZ12;
    TRUNCATE TABLE bronze.erp_LOC_A101;
    TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

    -- Load bronze.crm_cust_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Started loading bronze.crm_cust_info at %', start_time;
    COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    FROM 'C:/Users/Ridwan/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	GET DIAGNOSTICS row_count = ROW_COUNT;
    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;
    RAISE NOTICE 'Finished loading bronze.crm_cust_info at %. Duration: %', end_time, duration;
    RAISE NOTICE 'Loaded % rows into bronze.crm_cust_info', row_count;

    -- Load bronze.crm_sales_details
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Started loading bronze.crm_sales_details at %', start_time;
    COPY bronze.crm_sales_details (sls_ord_num,
	sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    FROM 'C:/Users/Ridwan/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', FORCE_NULL (sls_ord_num,
	sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price));
	GET DIAGNOSTICS row_count = ROW_COUNT;
    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;
    RAISE NOTICE 'Finished loading bronze.crm_sales_details at %. Duration: %', end_time, duration;
    RAISE NOTICE 'Loaded % rows into bronze.crm_sales_details', row_count;

    -- Load bronze.crm_prd_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Started loading bronze.crm_prd_info at %', start_time;
    COPY bronze.crm_prd_info
    FROM 'C:/Users/Ridwan/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	GET DIAGNOSTICS row_count = ROW_COUNT;
    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;
    RAISE NOTICE 'Finished loading bronze.crm_prd_info at %. Duration: %', end_time, duration;
    RAISE NOTICE 'Loaded % rows into bronze.crm_prd_info', row_count;

    -- Load bronze.erp_CUST_AZ12
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Started loading bronze.erp_CUST_AZ12 at %', start_time;
    COPY bronze.erp_CUST_AZ12
    FROM 'C:/Users/Ridwan/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	GET DIAGNOSTICS row_count = ROW_COUNT;
    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;
    RAISE NOTICE 'Finished loading bronze.erp_CUST_AZ12 at %. Duration: %', end_time, duration;
    RAISE NOTICE 'Loaded % rows into bronze.erp_CUST_AZ12', row_count;

    -- Load bronze.erp_LOC_A101
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Started loading bronze.erp_LOC_A101 at %', start_time;
    COPY bronze.erp_LOC_A101
    FROM 'C:/Users/Ridwan/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	GET DIAGNOSTICS row_count = ROW_COUNT;
    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;
    RAISE NOTICE 'Finished loading bronze.erp_LOC_A101 at %. Duration: %', end_time, duration;
    RAISE NOTICE 'Loaded % rows into bronze.erp_LOC_A101', row_count;

    -- Load bronze.erp_PX_CAT_G1V2
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Started loading bronze.erp_PX_CAT_G1V2 at %', start_time;
    COPY bronze.erp_PX_CAT_G1V2
    FROM 'C:/Users/Ridwan/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
	GET DIAGNOSTICS row_count = ROW_COUNT;
    end_time := CURRENT_TIMESTAMP;
    duration := end_time - start_time;
    RAISE NOTICE 'Finished loading bronze.erp_PX_CAT_G1V2 at %. Duration: %', end_time, duration;
    RAISE NOTICE 'Loaded % rows into bronze.erp_PX_CAT_G1V2', row_count;

    -- Capture end time and total duration for Bronze layer
    total_end_time := CURRENT_TIMESTAMP;
    total_duration := total_end_time - total_start_time;
    RAISE NOTICE 'Finished loading entire Bronze layer at %. Total Duration: %', total_end_time, total_duration;

    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred: %', SQLERRM;
        RAISE EXCEPTION 'Load failed due to error: %', SQLERRM; -- Re-raise to ensure failure is clear
END;
$$;



CALL bronze.load_bronze();