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
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DateTime, @end_time DateTime , @Start_batch_time DATETIME, @END_batch_time DATETIME 
	BEGIN TRY 
		SET @Start_batch_time = GETDATE()
		PRINT '=============================='
		PRINT ' Loading The silver Layer '
		PRINT '=============================='

		PRINT '=============================='
		PRINT ' Loading CRM Customer Table  '
		PRINT '=============================='

		-- Loading  Silver.crm_cust_info
		SET @start_time = GETDATE() 
		PRINT 'Truncating Customer Table silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info ;
		PRINT 'Insert clean data from bronze layer to Silver Layer : silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			[cst_id],
			[cst_key],
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date) 

		SELECT 
			[cst_id],
			[cst_key],

			-- Remove Unwanted spaces using TRIM Function
			TRIM([cst_firstname]) AS cst_firstname,
			TRIM([cst_lastname]) AS cst_lastname,

			CASE UPPER(TRIM(cst_marital_status))
				WHEN 'M' THEN 'Married'
				WHEN 'S' THEN 'Single'
				ELSE 'n/a'
			END cst_marital_status, -- Normalize Data into a readable format

			CASE  UPPER(TRIM(cst_gndr))
				WHEN 'F' THEN 'Female'
				WHEN 'M' THEN 'Male' 
				ELSE 'n/a'
			END cst_gndr, -- Normalize Data into a readable format

			[cst_create_date]
		FROM (
			SELECT 
			*,
			ROW_NUMBER()OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info 
			) t 
		WHERE flag_last = 1 AND cst_id IS NOT NULL -- Select the most recent data in duplicates
	SET @end_time = GETDATE();

	PRINT' ================================= '
	PRINT 'The duration of loading data for CRM customer table is: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR ) + ' second. '
	PRINT' ================================= '

		PRINT '=============================='
		PRINT ' Loading CRM product Table  '
		PRINT '=============================='

		-- TRUNCATING & INSERTING CLEAN DATA INTO SILVER PROD TABLE 
	SET @start_time = GETDATE()
		PRINT 'Truncating product Table silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT 'INSERTING CLEAN DATA INTO silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (
			[prd_id],
			[cat_id],
			[prd_key],
			[prd_nm],
			[prd_cost],
			[prd_line],
			[prd_start_dt],
			[prd_end_dt] )

		SELECT 
		prd_id,
		Replace(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- Extract category ID
		SUBSTRING(prd_key,7,len(Prd_key))  AS prd_key, -- Extract Product Key 
		prd_nm,
		coalesce(prd_cost,0) As prd_cost, -- Handling Nulls 
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'N/A'
		END prd_line,		-- Map product line codes to descriptive values
			  CAST( [prd_start_dt] AS DATE ) AS prd_start_dt, --	Handling 
			  CAST(
					LEAD(prd_start_dt) OVER ( PARTITION BY prd_key Order by prd_start_dt ) -1 
			  AS DATE )  AS prd_end_dt  -- Calculate end date as one day before the next start date

		  FROM [DataWarehouse].[bronze].[crm_prd_info]; 
	SET @end_time = GETDATE()
	PRINT ' ============================= '
	PRINT ' The duration of loading data for CRM product customer table is: : ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR ) + ' second. '
	PRINT '=============================='

		PRINT '=============================='
		PRINT ' Loading CRM Sales details Table  '
		PRINT '=============================='



		-- TRUNCATING AND INSERTING CRM Sales Details :
		SET @start_time = GETDATE()
		PRINT 'Truncating sales details Table silver.crm_Sales_Details'
		TRUNCATE TABLE silver.crm_sales_details ;
		PRINT'INSERTING CLEAN DATA Into CRM sales Details '
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price )

		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
		CASE 
			WHEN sls_order_dt < = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
			ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
		END sls_order_dt,  

		CASE 
			WHEN sls_ship_dt <=0 OR LEN(sls_ship_dt) != 8 THEN NULL 
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		END sls_ship_dt,
		CASE 
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL 
			ELSE CAST(CAST(sls_due_dt AS varchar ) AS DATE)
		END sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price )
			ELSE sls_sales
		End sls_sales,		-- Recalculate sales if original value is missing or incorrect
			sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity,0) -- USING NULL If sls_quantity is 0 
			Else sls_price 
		End sls_price -- derive price if original value is incorrect 
		FROM bronze.crm_sales_details
	SET @end_time = GETDATE()

	PRINT '=============================='
	PRINT 'The duration of loading data for CRM sales details table is:: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + 'Second '
	PRINT '=============================='

		PRINT '=============================='
		PRINT ' Loading ERP CUST AZ Table  '
		PRINT '=============================='


		-- Truncating & Inserting cleaned data into silver.erp_cust_az12
		SET @start_time = GETDATE()
		PRINT ' Truncating Silver.erp_cust_az12 '
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT ' inserting clean data into Silver.erp_cust_az12 '
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen)
		SELECT 
			CASE
				WHEN cid LIKE 'NAS%' THEN substring(cid,4,LEN(cid))
				ELSE cid
			END cid, -- Handling Invalid Data :Remove 'NAS' prefix if present
			CASE 
				WHEN bdate > GETDATE() THEN NULL 
				ELSE bdate
			END bdate,  -- Set future birthdates to NULL
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'N/A'
			END gen  -- Normalize data into a readable format 
		FROM bronze.erp_cust_az12
	SET @end_time = GETDATE();
	PRINT '=============================='
	PRINT' The duration of loading data for erp cust_az12 is : ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)
	PRINT '=============================='

		PRINT '=============================='
		PRINT ' Loading ERP location Table  '
		PRINT '=============================='


		-- Truncating And Inserting cleaned data into silver.erp_loc_a101
		SET @start_time = GETDATE()
		PRINT'Truncating silver.erp.loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT'Inserting clean data into Silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry )
		SELECT 
			Replace(cid,'-','')cid, 
			CASE 
				WHEN TRIM(cntry) IN ( 'USA','US') THEN 'United States'
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'N/A'
				ELSE cntry 
		END cntry  -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101

	SET @end_time = GETDATE() 
	PRINT '=============================='
	PRINT '  The duration of loading data for erp loc_a101 is :  ' +  CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + 'Second ';
	PRINT '=============================='


		PRINT '=============================='
		PRINT ' Loading ERP category Table  '
		PRINT '=============================='


		-- Truncating & loading cleaned date into silver.ERP_px_cat_g1v2
		SET @start_time = GETDATE()
		PRINT ' Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT ' Inserting Cleaned Data Into: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2 
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance 
		) 
		SELECT 
			id,
			cat,
			subcat,
			maintenance 
		FROM bronze.erp_px_cat_g1v2 
	SET @end_time = GETDATE()
	PRINT '=============================='
	PRINT ' The duration of loading data for erp_px_cat_g1v2 is :  : ' + CAST(DATEDIFF(second,@start_time,@end_time )AS NVARCHAR) + ' Second ';
	PRINT '=============================='
SET @END_batch_time = GETDATE()
PRINT '=============================='
PRINT 'The batch duration time is : ' + CAST(DATEDIFF(second,@Start_batch_time,@end_batch_time)AS NVARCHAR) + ' second ' ;
PRINT '=============================='
END TRY 
	-- ADDIND CATCH PART FOR HANDLING ERRORS 
	BEGIN CATCH 
	PRINT '=============================='
	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER '
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT ' ERROR LINE ' + CAST(ERROR_LINE()AS NVARCHAR );
	PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR STATE ' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '=============================='
	END CATCH 
END;
