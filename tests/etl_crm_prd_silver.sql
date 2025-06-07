/*
CLEAN & LOAD EACH TABLE (crm_prd_info)
1. Check for Nulls or duplicates in Primary Key: To detect any duplicates, we need to aggregate the Primary Key
to find any value in the PK that exist more than once. The Primary key must be unique and not null.
*/
		SELECT
		prd_id,
		COUNT(*)
		FROM bronze.crm_prd_info
		GROUP BY prd_id
		HAVING COUNT(*) > 1 OR prd_id IS NULL

	--- Everything looks great. No duplicates or Null. 

/*
2. Derive new Columns from one column: Here, we will split the columns (prd_key) into two information.
    >> WE EXTRACT THE FIRST PART OF THE prd_key COLUMN<<
In this specific column, the first five characters is the category ID. 
So we will use the SUBSTRING function (Extracts a specific part of a string value). It has three arguments: 
    >> a) The column you want to extract from
	>> b) Define the position where to extract
	>> c) Specify the length ie how many characters you want to extract
*/
		SELECT 
		prd_id,
		prd_key,
		SUBSTRING(prd_key, 1, 5) AS cat_id,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info

--- To double check the new column called category ID with the category ID from the ERP Table.
--- The output of the extracted column is to join it with this table, hence we double check.
		SELECT DISTINCT id
		FROM bronze.erp_px_cat_g1v2

--- Everything aligns except that the we have an underscore between the category and subcategory in the ID column (ERP table)
--- but the derived column has a hyphen so we have to replace it with an underscore inorder to have matching information btwn both tables.
--- Use the REPLACE function
		SELECT 
		prd_id,
		prd_key,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info

--- Before joining both tables, we need to check if we have matching data in both tables. In order words, we are trying to find any category ID not available in the second table.
---In essence, we want to filter out unmatched data after applying transformation. The CO_PE category ID is not available in the second table.
		SELECT 
		prd_id,
		prd_key,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info
		WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
		(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)



-->> WE EXTRACT THE SECOND PART OF THE prd_key COLUMN<< (Factoring in the REPLACE function for the hyphen & underscores).
-->> Now, because in the last argument, some columns have different lengths than the other, not fixed. 
-->> We have to make the argument dynamic and not fixed by using LEN function to return the number of eact characters in a string.
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info

--- To double check the new column called product key with the product key from the Sales detail Table.
--- The output of the extracted column is to join it with this table, hence we double check.
		SELECT DISTINCT sls_prd_key
		FROM bronze.crm_sales_details

--- Before joining both tables, we need to check if we have matching data in both tables.
---In essence, we want to filter out unmatched data after applying transformation. Use IN instead of NOT IN.
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info
		WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN 
		(SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details)

/*
3. Check for Unwanted spaces in string values: The easy way is to filter using the WHERE clause and the TRIM function.
Here, we are saying the first name is not equal to the first name after trimming the values. Using the TRIM function removes all leading and trailing
spaces so if the original value is not equal to the same value after trimming it, it means there are spaces. Do this for all the columns.
*/
		SELECT prd_nm
		FROM bronze.crm_prd_info
		WHERE prd_nm != TRIM(prd_nm) --- Everything looks great. No unwanted spaces in this column. 


/*
4. Check for Nulls or Negative Numbers. Here we check for negative prices/costs which isnt realistic depending on the business
*/
		SELECT prd_cost
		FROM bronze.crm_prd_info
		WHERE prd_cost < 0 OR prd_cost IS NULL 
	
--- Write the query script that will clean/transform the data
---From the output, we have only nulls and no negative numbers so we replace nulls with a zero.
--- We will use the ISNULL function to replace Null values with a specified replacement value.
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info



/*
5. Data Standardization and Consistency: Check the consistency of values in low cardinality columns (prd_line column).
These are considered columns with limited number of possible values inside the columns. 
In our data warehouse, we aim to store clear and meaningful values rather than using abbreviated terms. 
  >> We will use dafault value 'n/a' for missing values. 
  >> Apply UPPER function just in case mixed-case values appear later in your column.
  >> Apply TRIM function just in case spaces appear later in your column.
*/
		SELECT DISTINCT prd_line
		FROM bronze.crm_prd_info


	--- Write the query script that will clean/transform the data
	--- In our data warehouse, we aim to store clear and meaningful values rather than using abbreviated terms. 
          -- >> We will use dafault value 'n/a' for missing values. 
		  -- >> Use CASE WHEN Statements to map the abbrvaitions to clear meaningful terms. 
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE WHEN prd_line = 'M' THEN 'Mountain'
		     WHEN prd_line = 'R' THEN 'Road'
			 WHEN prd_line = 'S' THEN 'Other Sales'
			 WHEN prd_line = 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info
		

	-- >> Apply UPPER function just in case mixed-case values appear later in your column.
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE WHEN UPPER(prd_line) = 'M' THEN 'Mountain'
		     WHEN UPPER(prd_line) = 'R' THEN 'Road'
			 WHEN UPPER(prd_line) = 'S' THEN 'Other Sales'
			 WHEN UPPER(prd_line) = 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info

	-- >> Apply TRIM function just in case spaces appear later in your column to remove unwanted spaces.
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info
		
--===================================================================================================--
--In a situation where we have a simple mapping we can use quick CASE WHEN like below.
--This is only for simple mapping and not for complex conditions.
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) --- We are evaluating this value so no need to keep repeating it
			WHEN 'M' THEN 'Mountain'
		    WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		prd_end_dt
		FROM bronze.crm_prd_info
--===================================================================================================--

/*
6. Check for Invalid Date Orders (prd_start_dt and prd_end_dt).
N/B: >> The end date must not be earlier than the start date.
     >> Each record must have a start date. It is okay to have a start without an end date.
*/
		SELECT *
		FROM bronze.crm_prd_info
		WHERE prd_end_dt < prd_start_dt

--- Write the query script that will clean/transform the data
-- >>> LOGIC: To transform the date orders, we must ensure that the  >> End date = Start date of the 'Next' Record -1  <<<
-- In SQL, if you are at a paricular record and you want to access another information from another record, Use LEAD & LAG Function.
-- The LEAD function accesses values from the next row within a window.
/* 
Lets breakdown the Logic script for transforming invalid date:
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
Here, we need the LEAD of the start date and also want the start date of the next records so we write (OVER). We then
(PARTITION BY) the data so the window will focus on only one product being the (prd_key) ie we divide the data by the prd_key.
We then sort by (ORDER BY) by the start date in ascending order from the lowest to the highest. Then name the script (prod_end_dt).
We now want to set the end date to be the previous day, we include (-1) to avoid overlapping between the start and the end date.
*/
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) --- We are evaluating this value so no need to keep repeating it
			WHEN 'M' THEN 'Mountain'
		    WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt
		FROM bronze.crm_prd_info

--- To remove the time information since they are all zeros, we use the CAST function and make both columns as a date rather than a date time.
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) --- We are evaluating this value so no need to keep repeating it
			WHEN 'M' THEN 'Mountain'
		    WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info


/* 
Finally, write the INSERT INTO Statement after you have identified and cleaned the data.
We have now inserted clean data inside the silver table.
*/
		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --- Extract category ID
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,		   --- Extract product key
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) --- We are evaluating this value so no need to keep repeating it
			WHEN 'M' THEN 'Mountain'
		    WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line, --- Map product line codes to descriptive values
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 
			AS DATE
			) AS prd_end_dt --- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info

/*
QUALITY CHECK FOR THE SILVER TABLE
Rerun the quality check queries from the bronze layer to verify the quality of data in the silver layer.
*/
   --->> CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
		SELECT
		prd_id,
		COUNT(*)
		FROM silver.crm_prd_info
		GROUP BY prd_id
		HAVING COUNT(*) > 1 OR prd_id IS NULL

  --->> CHECK FOR UNWANTED SPACES IN STRING VALUES
		SELECT prd_nm
		FROM silver.crm_prd_info
		WHERE prd_nm != TRIM(prd_nm)

   --->> CHECK FOR NULLS OR NEGATIVE NUMBERS
   		SELECT prd_cost
		FROM silver.crm_prd_info
		WHERE prd_cost < 0 OR prd_cost IS NULL
			   		 	  
 --->> DATA STANDARDIZATION AND CONSISTENCY IN LOW CARDINALITY COLUMNS
		SELECT DISTINCT prd_line
		FROM silver.crm_prd_info

  --->> CHECK FOR INVALID DATE ORDERS
		SELECT *
		FROM silver.crm_prd_info
		WHERE prd_end_dt < prd_start_dt
	   	  
---=======FINALLY CHECK THE ENTIRE SILVER TABLE=============
		SELECT *
		FROM silver.crm_prd_info
