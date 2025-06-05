/*
DEVELOP SQL LOAD SCRIPTS
To avoid loading the file/data twice into the table, It is advisable to use the TRUNCATE TABLE AND then BULK INSERT. 
Now, we write SQL Bulk Insert to load all csv files into the Bronze table.
*/
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


--Test the quality of your table and the data
--The Customer Information Table
SELECT *
FROM bronze.crm_cust_info;

SELECT COUNT(*)
FROM bronze.crm_cust_info;

--The Product Information Table
SELECT *
FROM bronze.crm_prd_info;

SELECT COUNT(*)
FROM bronze.crm_prd_info;

--The Sales Detail Table
SELECT *
FROM bronze.crm_sales_details;

SELECT COUNT(*)
FROM bronze.crm_sales_details;

--The Customer AZ Table
SELECT *
FROM bronze.erp_cust_az12;

SELECT COUNT(*)
FROM bronze.erp_cust_az12;

--The Location A1 Table
SELECT *
FROM bronze.erp_loc_a101;

SELECT COUNT(*)
FROM bronze.erp_loc_a101;

--The Price Table
SELECT *
FROM bronze.erp_px_cat_g1v2;

SELECT COUNT(*)
FROM bronze.erp_px_cat_g1v2;
