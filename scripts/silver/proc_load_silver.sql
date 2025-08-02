/*
============================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
============================================================================================
Script Purpose:
  This stored procedure performs the ETL(Extract, Transform, Load) process to
  Populate the 'silver' schema tables from the 'bronze' schema.
  It performs the following actions:
    - Truncates the silver tables
    - Inserted transformed and cleansed data from Bronze into Silver tables

Parameters:
 None
 This stored procedure does not accept any parameters or return any values

Usage Example:
   EXEC silver.load_silver;
=============================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME

	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE()
		print '>> Truncating Data Inyo: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		print '>> Inserting Data Inyo: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_first_name,
		cst_last_name,
		cst_material_status,
		cst_gndr,
		cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_first_name) AS cst_first_name,
		TRIM(cst_last_name) AS cst_last_name,
		CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_material_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM(
			SELECT
			*,
			ROW_NUMBER() over(partition by cst_id order by cst_create_date DESC) as flag_last
			from bronze.crm_cust_info
			Where cst_id IS NOT NULL
		) t where flag_last = 1
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE()
		print '>> Truncating Data Inyo: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		print '>>Inserting Data Inyo: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		select 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		DATEADD(Day,-1 ,LEAD(prd_start_dt) over(PARTITION BY prd_key order by prd_start_dt)) AS prd_end_dt
		from bronze.crm_prd_info
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE()
		print '>> Truncating Data Inyo: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		print '>>Inserting Data Inyo: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt, 
			sls_sales,
			sls_quantity,
			sls_price
		)
		select
		sls_ord_num,
		sls_prd_key, 
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END sls_order_dt,

		CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END sls_ship_dt,

		CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END sls_due_dt,
		CASE WHEN sls_sales IS NULL or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL or sls_price <= 0
				THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price

		from bronze.crm_sales_details
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE()
		print '>> Truncating Data Inyo: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		print '>>Inserting Data Inyo: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		select
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			 ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE 
			 WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
			 ELSE 'n/a'
		END As gen
		from bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE()
		print '>> Truncating Data Inyo: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		print '>>Inserting Data Inyo: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(cid,cntry)
		select
		REPLACE(cid,'-','') as cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
			 ELSE cntry
		END AS cntry
		from bronze.erp_loc_a101
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE()
		print '>> Truncating Data Inyo: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		print '>>Inserting Data Inyo: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '-------------------------------------------------';
		SET @batch_end_time = GETDATE()
		PRINT 'All data Loaded in silver layer'
		PRINT '>> Whole Batch Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
	END TRY
	BEGIN CATCH
		PRINT '=====================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================';
	END CATCH
END
