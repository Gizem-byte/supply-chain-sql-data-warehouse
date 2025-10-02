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

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY

		SET @batch_start_time= GETDATE();
		PRINT '========================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '========================================================';

		PRINT '-----------------------------------------------------------';
		PRINT 'Loading Main Table';
		PRINT '-----------------------------------------------------------';

		SET @start_time= GETDATE();
		PRINT '>> Truncating: bronze.dataco_supply_chain';
		TRUNCATE TABLE bronze.dataco_supply_chain;

		PRINT '>> Inserting Data Into: bronze.dataco_supply_chain';
		BULK INSERT bronze.dataco_supply_chain
		FROM 'C:\Users\gizem\Desktop\dataco\dataco_supply_chain.csv'
		WITH (
		   FIRSTROW = 2,
		   FIELDTERMINATOR = ',',
		   TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------------------------------';


		PRINT '-----------------------------------------------------------';
		PRINT 'Loading Clickstream Table';
		PRINT '-----------------------------------------------------------';

		SET @start_time= GETDATE();
		PRINT '>> Truncating: bronze.tokenized_access_logs';
		TRUNCATE TABLE bronze.tokenized_access_logs;

		PRINT '>> Inserting Data Into: bronze.tokenized_access_logs';
		BULK INSERT bronze.tokenized_access_logs
		FROM 'C:\Users\gizem\Desktop\dataco\tokenized_access_logs.csv'
		WITH (
		   FIRSTROW = 2,
		   FIELDTERMINATOR = ',',
		   TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------------------------------------';

		SET @batch_end_time= GETDATE();
		PRINT '=====================================================================';
		PRINT 'LOADING BRONZE LAYER IS COMPLETED';
		PRINT '>> TOTAL LOAD DURATION:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=====================================================================';


	END TRY
	BEGIN CATCH
		PRINT '===================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================';		
	END CATCH
END
