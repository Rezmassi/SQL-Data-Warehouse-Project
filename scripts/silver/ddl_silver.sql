/*
 * ******************************************************************************************
 * DDL SCRIPT: Create silver Tables
 * ******************************************************************************************
 * Script Purpose: This script creates tables in the silver schema, dropping existing table if they already exist.
 * It also adjusts for Postgres variable declaration.
 * Similar to Bronze DDL, but with added column for metadata. 
 * These columns are crucial for tracking the lineage, quality, and processing history of the data.
 * Run this script to redefine the DDL structure of the 'silver' tables.
 */

--naming convention = source_system->entity

-- Drop the existing table
DROP TABLE silver.crm_cust_info;


CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
    dwh_create_date timestamp without time zone DEFAULT now()
);


select* from silver.crm_sales_details;
drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date timestamp without time zone DEFAULT now()
);

drop table silver.crm_prd_info;
create table silver.crm_prd_info(
prd_id int,
cat_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date timestamp without time zone DEFAULT now()
);


create table silver.erp_cust_az12(
CID varchar(50),
BDATE date,
GEN varchar(50),
dwh_create_date timestamp without time zone DEFAULT now()
);


create table silver.erp_loc_a101(
CID varchar(50),
CNTRY varchar(50),
dwh_create_date timestamp without time zone DEFAULT now()
);


create table silver.erp_px_cat_g1v2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50),
dwh_create_date timestamp without time zone DEFAULT now()
);

