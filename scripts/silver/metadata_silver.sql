/*
ADD METADATA COLUMNS TO THE DDL SCRIPT FOR SILVER TABLES.
These are additional columns added to each table to provide extra information for each record.
N/B: In the silver layer, sometimes we need to adjust the metadata if the quality of the data types is not good.
or we are building new derived information in order to integrate the data into the table.
*/

IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
		DROP TABLE silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info (
		cst_id INT,
		cst_key NVARCHAR (50),
		cst_firstname NVARCHAR (50),
		cst_lastname NVARCHAR (50),
		cst_marital_status NVARCHAR (50),
		cst_gndr NVARCHAR (50),
		cst_create_date DATE,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
		DROP TABLE silver.crm_prd_info;
GO
/*---Since we had to derive another column by spliting the column, we need to include the new column in the DDL
and also we changed the data type of start date and end date to DATE. We need to effect the chnages in the DDL.
CREATE TABLE silver.crm_prd_info(
		prd_id INT,
		prd_key NVARCHAR (50),
		prd_nm NVARCHAR (50),
		prd_cost INT,
		prd_line NVARCHAR (50),
		prd_start_dt DATETIME,
		prd_end_dt DATETIME,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
);
*/
CREATE TABLE silver.crm_prd_info(
		prd_id INT,
		cat_id NVARCHAR(50),
		prd_key NVARCHAR (50),
		prd_nm NVARCHAR (50),
		prd_cost INT,
		prd_line NVARCHAR (50),
		prd_start_dt DATE,
		prd_end_dt DATE,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
		DROP TABLE silver.crm_sales_details;
GO
/*Since we changed the data types of the date columns from Integer to Date, we have effect the changes in the DDL.
CREATE TABLE silver.crm_sales_details(
		sls_ord_num NVARCHAR (50),
		sls_prd_key NVARCHAR (50),
		sls_cust_id INT,
		sls_order_dt INT,
		sls_ship_dt INT,
		sls_due_dt INT,
		sls_sales INT,
		sls_quantity INT,
		sls_price INT,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
);
*/
CREATE TABLE silver.crm_sales_details(
		sls_ord_num NVARCHAR (50),
		sls_prd_key NVARCHAR (50),
		sls_cust_id INT,
		sls_order_dt DATE,
		sls_ship_dt DATE,
		sls_due_dt DATE,
		sls_sales INT,
		sls_quantity INT,
		sls_price INT,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
		DROP TABLE silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12(
		cid NVARCHAR (50),
		bdate DATE,
		gen NVARCHAR (50),
		dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
		DROP TABLE silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101(
		cid NVARCHAR (50),
		cntry NVARCHAR (50),
		dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
		DROP TABLE silver.erp_px_cat_g1v2;
GO
CREATE TABLE silver.erp_px_cat_g1v2(
		id NVARCHAR (50),
		cat NVARCHAR (50),
		subcat NVARCHAR (50),
		maintenance NVARCHAR (50),
		dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
