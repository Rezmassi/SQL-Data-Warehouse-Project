/*
 * ******************************************************************************************
 * DDL SCRIPT: Create Bronze Tables
 * ******************************************************************************************
 * Script Purpose: This script creates tables in the bronze schema, dropping existing table if they already exist.
 * It also adjusts for Postgres variable declaration.
 * Run this script to redefine the DDL structure of the 'Bronze' tables.
 */

--naming convention = source_system->entity

-- Drop the existing table
DROP TABLE bronze.crm_cust_info;


CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);



drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num text,
sls_prd_key text,
sls_cust_id text,
sls_order_dt text,
sls_ship_dt text,
sls_due_dt text,
sls_sales text,
sls_quantity text,
sls_price text
);



DROP TABLE bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id int,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date
);


create table bronze.erp_cust_az12(
CID varchar(50),
BDATE date,
GEN varchar(50)
);


create table bronze.erp_loc_a101(
CID varchar(50),
CNTRY varchar(50)
);


create table bronze.erp_px_cat_g1v2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50)
);

