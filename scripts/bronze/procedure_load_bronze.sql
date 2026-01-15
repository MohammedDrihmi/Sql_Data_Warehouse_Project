/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze  AS 
BEGIN 
	DECLARE @start_time date , @end_time datetime, @batch_start_time DATETIME, @batch_end_time DATETIME ;
-- TRY PART : HANDLING ERRORS 
	BEGIN TRY 
		SET @batch_start_time = GETDATE()
		PRINT '========================='
		PRINT'loading the bronze layer';
		PRINT '========================='

		Print ' --------------------------------------'
		PRINT 'Loading CRM  tables ' ;
		PRINT '--------------------------------------'


			-- INSERT to crm cust_info 
		PRINT ' >> Truncating table bronze.crm_cust_info ';
		PRINT ' >> Inserting Data Into : bronze.crm_cust_info' 


		SET @start_time = GETDATE() 
		TRUNCATE TABLE bronze.crm_cust_info ;
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\lenovo\OneDrive\Bureau\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' -- Path
		WITH ( -- HOW TO HANDLE THE PATH 
			FIRSTROW = 2,   -- Data start from 2nd row 1row is header column name 
			FIELDTERMINATOR =',', -- delimiter 
			TABLOCK			-- while load block the table 
				  );
		SET @end_time = GETDATE()	
		Print ' THE DURATION IS : ' + CAST( DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' Seconds' ;
		Print ' ---------------------------------------------------------------------------------------------'

		PRINT ' >> Truncating table bronze.crm_prd_info ';
		PRINT ' >> Inserting Data Into : bronze.crm_prd_info' 


		-- INSERT to crm pro_info  
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;

		BULK INSERT bronze.crm_prd_info 
		FROM 'C:\Users\lenovo\OneDrive\Bureau\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
		SET @end_time = GETDATE() 
		PRINT ' the duration is : ' + CAST(DATEDIFF(SECONF,@start_time,@end_time) as NVARCHAR) + ' Seconds ';
		Print ' --------------------------------------------------------------------------------------------';

		-- INSERT SALES DETAILS 
		PRINT ' >> Truncating table bronze.crm_sales_details ';
		PRINT ' >> Inserting Data Into : bronze.crm_sales_details' 

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_sales_details

		BULK INSERT bronze.crm_sales_details 
		FROM 'C:\Users\lenovo\OneDrive\Bureau\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE() 
		PRINT 'the duration is ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR ) + ' seconds' ; 
		PRINT '--------------------------------------------------------------------------------------------'; 


		PRINT ' --------------------------------------'
		PRINT 'Loading ERP  tables ' ;
		PRINT '--------------------------------------'


		-- INSERT ERP CUST AZ 
		PRINT ' >> Truncating table bronze.erp_cust_az12 ';
		PRINT ' >> Inserting Data Into : bronze.erp_cust_az12';

		SET @start_time =GETDATE()
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12 
		FROM 'C:\Users\lenovo\OneDrive\Bureau\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR =',',
			TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT 'The duration is : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR ) + 'Seconds'; 
		PRINT '------------------------------------------------------------------------------------------- ';
			

		-- INSERT ERP LOC A101
		PRINT ' >> Truncating table [bronze].[erp_loc_a101] ';
		PRINT ' >> Inserting Data Into :[bronze].[erp_loc_a101]';

		SET @start_time = GETDATE() ;

		TRUNCATE TABLE [bronze].[erp_loc_a101]
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\Users\lenovo\OneDrive\Bureau\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time =GETDATE(); 
		PRINT ' the duration is : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR ) + ' Seconds';


		-- INSERT PX CAT G1V2 
		PRINT ' >> Truncating table [bronze].[erp_px_cat_g1v2] ';
		PRINT ' >> Inserting Data Into : [bronze].[erp_px_cat_g1v2] ';

		SET @start_time = GETDATE()

		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\Users\lenovo\OneDrive\Bureau\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT 'The duration is : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds' ;
		PRINT ' --------------------'

		SET @batch_end_time = GETDATE()
		PRINT ' LOADING BRONZE LAYER IS COMPLETED ';
		PRINT ' THE BATCH DURATION IS : ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)as NVARCHAR ) + 'SECONDS';

	END TRY 
	BEGIN CATCH 
		PRINT '----------------------------';
		PRINT'ERROR OCCURED DURING LOADING';
		PRINT'ERROR MESSAGE'+ ERROR_MESSAGE();
		PRINT'ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------';
	END CATCH 
END 
GO

-- EXECUTION 
EXECUTE bronze.load_bronze ;
GO
