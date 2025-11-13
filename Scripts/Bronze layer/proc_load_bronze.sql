/*
============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
============================================================

Script Purpose:
	This stored procedure loads Data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncate the bronze table before loading data.
	- uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze;

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_date DATETIME,@end_date DATETIME, @batch_start_time DATETIME , @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================'

		PRINT '--------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------';

		SET @start_date = GETDATE();
		PRINT '>> Truncating TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\shahbaz\Desktop\Study materials\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + 'seconds'
		PRINT '--------------';

		SET @start_date = GETDATE();
		PRINT '>> Truncating TABLE: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\shahbaz\Desktop\Study materials\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + 'seconds'
		PRINT '--------------';

		SET @start_date = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\shahbaz\Desktop\Study materials\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + 'seconds'
		PRINT '--------------';

		PRINT '--------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------';

		SET @start_date = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT '>> Inserting Data Into: bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\shahbaz\Desktop\Study materials\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + 'seconds'
		PRINT '--------------';

		SET @start_date = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT '>> Inserting Data Into: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\shahbaz\Desktop\Study materials\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + 'seconds'
		PRINT '--------------';

		SET @start_date = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT '>> Inserting Data Into: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\shahbaz\Desktop\Study materials\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + 'seconds'
		PRINT '--------------';

		SET @batch_end_time = GETDATE();
		PRINT '================================';
		PRINT 'Bronze Layer Loading Completed'
		PRINT 'Total Load Duration: '+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
		PRINT '================================';
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR OCCURED WHILE LOADING Bronze Layer';
		PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================';
	END CATCH
END
