/*
ERROR HANDLING IN STORED PROCEDURES
To handle errors in Stored Procedures, Add TRY .....CATCH. It ensures error handling, data integrity and issues logging for easier debugging.
	BEGIN TRY
	END TRY
	BEGIN CATCH
	END CATCH
SQL will run the TRY block and if it fails, it runs the CATCH block to handle the error. This means that the CATCH will only be executed if SQL 
failed to run the TRY. Having done that, we need to define for SQL what to do if there's an error in our code.
To do this we Add PRINT inbetween the BEGIN CATCH and END CATCH.
Also, it is important to Add the Duration of each step to track ETL duration. It helps to identify bottlenecks, optimize performance,
monitor trends and detect issues.
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		PRINT '=====================================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=====================================================================================';

		PRINT '--------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------------------------------------';
	
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' -- The location of the file in the system
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' -- The location of the file in the system
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

	
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
	
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' -- The location of the file in the system
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		PRINT '--------------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------------------------------------------';
	
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
	
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv' -- The location of the file in the system
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

	
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv' -- The location of the file in the system
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

	
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv' -- The location of the file in the system
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	END TRY
	BEGIN CATCH
		PRINT '=====================================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================================================================';
	END CATCH
END

--Next, execute the Stored Procedure
EXEC bronze.load_bronze
