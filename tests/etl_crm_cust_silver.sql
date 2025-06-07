/*
CLEAN & LOAD EACH TABLE (crm_cust_info)
1. Check for Nulls or duplicates in Primary Key: To detect any duplicates, we need to aggregate the Primary Key
to find any value in the PK that exist more than once. The Primary key must be unique and not null.
*/
		SELECT
		cst_id,
		COUNT(*)
		FROM bronze.crm_cust_info
		GROUP BY cst_id
		HAVING COUNT(*) > 1 OR cst_id IS NULL

	--- Write the query script that will clean/transform the data
	-- This particular customer occured three times so we have the pick the correct one which is the most recent looking at the timestamp
		SELECT *
		FROM bronze.crm_cust_info
		WHERE cst_id = 29466 

	--- Now, we need to rank all the values based on the create_date and only pick the highest one. We use the RANKING function.
	---Use the function ROW_NUMBER function, OVER and PARTITION BY.
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id = 29466

		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info

		SELECT *
		FROM(
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		)t WHERE flag_last !=1

		SELECT *
		FROM(
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		)t WHERE flag_last = 1 

/*CLEAN & LOAD EACH TABLE.
2. Check for Unwanted spaces in string values: The easy way is to filter using the WHERE clause and the TRIM function.
Here, we are saying the first name is not equal to the first name after trimming the values. Using the TRIM function removes all leading and trailing
spaces so if the original value is not equal to the same value after trimming it, it means there are spaces. Do this for all the columns.
*/
		SELECT cst_firstname
		FROM bronze.crm_cust_info
		WHERE cst_firstname != TRIM(cst_firstname)

		SELECT cst_lastname
		FROM bronze.crm_cust_info
		WHERE cst_lastname != TRIM(cst_lastname)

	--- Write the query script that will clean/transform the data
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS  cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		FROM(
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1 

/*CLEAN & LOAD EACH TABLE.
3. Data Standardization and Consistency: Check the consistency of values in low cardinality columns (Gender and Marital Status columns).
These are considered columns with limited number of possible values inside the columns. 
In our data warehouse, we aim to store clear and meaningful values rather than using abbreviated terms. 
  >> We will use dafault value 'n/a' for missing values. 
  >> Apply UPPER function just in case mixed-case values appear later in your column.
  >> Apply TRIM function just in case spaces appear later in your column.
*/
		SELECT DISTINCT cst_gndr
		FROM bronze.crm_cust_info

		SELECT DISTINCT cst_marital_status
		FROM bronze.crm_cust_info

	--- Write the query script that will clean/transform the data
	--- In our data warehouse, we aim to store clear and meaningful values rather than using abbreviated terms. 
          -- >> We will use dafault value 'n/a' for missing values. 
		  -- >> Use CASE WHEN Statements to map the abbrvaitions to clear meaningful terms. 
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS  cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN cst_marital_status = 'S' THEN 'Single'
			 WHEN cst_marital_status = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN cst_gndr = 'F' THEN 'Female'
			 WHEN cst_gndr = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM(
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1 

	-- >> Apply UPPER function just in case mixed-case values appear later in your column.
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS  cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
			 WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
			WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM(
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1 

	-- >> Apply TRIM function just in case spaces appear later in your column to remove unwanted spaces.
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS  cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM(
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1 


/* 
Finally, write the INSERT INTO Statement after you have identified and cleaned the data.
We have now inserted clean data inside the silver table.
*/
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS  cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM(
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1 

/*
QUALITY CHECK FOR THE SILVER TABLE
Rerun the quality check queries from the bronze layer to verify the quality of data in the silver layer.
*/
   --->> CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
		SELECT
		cst_id,
		COUNT(*)
		FROM silver.crm_cust_info
		GROUP BY cst_id
		HAVING COUNT(*) > 1 OR cst_id IS NULL

  --->> CHECK FOR UNWANTED SPACES IN STRING VALUES
		SELECT cst_firstname
		FROM silver.crm_cust_info
		WHERE cst_firstname != TRIM(cst_firstname)

		SELECT cst_lastname
		FROM silver.crm_cust_info
		WHERE cst_lastname != TRIM(cst_lastname)

 --->> DATA STANDARDIZATION AND CONSISTENCY IN LOW CARDINALITY COLUMNS
		SELECT DISTINCT cst_gndr
		FROM silver.crm_cust_info

		SELECT DISTINCT cst_marital_status
		FROM silver.crm_cust_info

---=======FINALLY CHECK THE ENTIRE SILVER LAYER=============
		SELECT *
		FROM silver.crm_cust_info
