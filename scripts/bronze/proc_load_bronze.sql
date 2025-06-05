/* 
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Create STORED PROCEDURE for frquently used scripts. 
We will now create Stored Procedures from the Bulk insert scripts with had written earlier.
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' -- The location of the file in the system
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' -- The location of the file in the system
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' -- The location of the file in the system
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.erp_cust_az12
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv' -- The location of the file in the system
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv' -- The location of the file in the system
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\amand\Music\Data Analysis Bootcamp\SQL\Materials\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv' -- The location of the file in the system
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
END

--Next, execute the Stored Procedure
EXEC bronze.load_bronze
