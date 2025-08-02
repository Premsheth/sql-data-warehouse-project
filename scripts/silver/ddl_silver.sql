/*
============================================================================================
DDL Scripts: Create Silver Tables
============================================================================================
Script Purpose:
  This script creates tables in the 'silver' schema, dropping existing tables
  if they already exist.
  Run this script to re-define the DDL structure of 'silver' Tables
=============================================================================================
*/

IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info
CREATE TABLE silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_first_name nvarchar(50),
	cst_last_name nvarchar(50),
	cst_material_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date DATETIME2 DEFAULT GETDATE(),

) 

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info
CREATE TABLE silver.crm_prd_info(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
) 

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details
CREATE TABLE silver.crm_sales_details(
	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id int,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date DATETIME2 DEFAULT GETDATE(),
)

IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101
CREATE TABLE silver.erp_loc_a101(
cid VARCHAR(50),
cntry VARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE(),
);

IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12
CREATE TABLE silver.erp_cust_az12(
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE(),
);

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2
CREATE TABLE silver.erp_px_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE(),
);
