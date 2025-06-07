/*
CLEAN & LOAD EACH TABLE (crm_sales_details)
1. Check for Unwanted spaces in string values: The easy way is to filter using the WHERE clause and the TRIM function.
Here, we are saying the first name is not equal to the first name after trimming the values. Using the TRIM function removes all leading and trailing
spaces so if the original value is not equal to the same value after trimming it, it means there are spaces. Do this for all the columns.
*/
		SELECT 
		sls_ord_num
		FROM bronze.crm_sales_details
		WHERE sls_ord_num != TRIM(sls_ord_num) --- Everything looks great. No unwanted spaces in this column. 

/*
2. Change Data types: Here we want to convert Integer to Date.
	WE CANNOT CONVERT AN INTERGER TO A DATE IN SQL.
	YOU NEED TO FISRT CONVERT TO A VARCHAR AND THEN FROM VARCHAR, YOU CONVERT TO DATE.
    >>>> The first script: Please note that Negative numbers or zeros cannot be cast to a date. We dont have negative but we have zeros.
    >>>> The second script: shows us that there are values within the column that the length of characters isnt 8.
		 As we can see, the length of the values in this column ie the date must be 8. The output are strange values that cannot be convereted to a date.
    >>>> The third script checks for outliers by validating the boundaries of the date range.
	>>>> The fourth script merges them all.
*/
		SELECT 
		sls_order_dt
		FROM bronze.crm_sales_details
		WHERE sls_order_dt <= 0 

		SELECT 
		sls_order_dt
		FROM bronze.crm_sales_details
		WHERE LEN(sls_order_dt) != 8

		SELECT 
		sls_order_dt
		FROM bronze.crm_sales_details
		WHERE sls_order_dt > 20500101 

		SELECT							--- Quality check for Order Date
		sls_order_dt 
		FROM bronze.crm_sales_details
		WHERE sls_order_dt <= 0 
		OR LEN(sls_order_dt) != 8 
		OR sls_order_dt > 20500101

		SELECT							--- Quality check for Shipping Date
		sls_ship_dt
		FROM bronze.crm_sales_details
		WHERE sls_ship_dt <= 0 
		OR LEN(sls_ship_dt) != 8 
		OR sls_ship_dt > 20500101

		SELECT							--- Quality check for Due Date
		sls_due_dt
		FROM bronze.crm_sales_details
		WHERE sls_due_dt <= 0 
		OR LEN(sls_due_dt) != 8 
		OR sls_due_dt > 20500101


		--- Write the query script that will clean/transform the data
   --->> To handle the zeros in the column, we need to replace them with null using NULLIF function.
   --->> This function returns NULL if two given values are equal, otherwise it returns the first expression.
		SELECT 
		NULLIF(sls_order_dt,0) sls_order_dt --- If it is zero, make it null
		FROM bronze.crm_sales_details
		WHERE sls_order_dt <= 0 

	--->> To handle the strange values that cannot be converted to a date and then convert Interger to date, we do this.
	--->> However, note this:
	--->> WE CANNOT CONVERT AN INTERGER TO A DATE IN SQL.
	--->> YOU NEED TO FISRT CONVERT TO A VARCHAR AND THEN FROM VARCHAR, YOU CONVERT TO DATE.
		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		FROM bronze.crm_sales_details

/*
3. Check for Invalid Date Orders (sls_order_dt and sls_ship_dt).
N/B: >> The Order date must always be earlier than the Shipping date or Due date.
     >> It makes no sense if you're delivering an item without an order. So, the Order should happen then we ship the items. 
*/
		SELECT *
		FROM bronze.crm_sales_details
		WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt   --- Everything looks great

/*
4. Check for Nulls or Negative Numbers. Here we check for negative prices/costs which isnt realistic depending on the business
   >>> Business rules:
						Sales = Quantity * Price
		This means that these information must be positive values and negative, zeros and Nulls are not allowed.	
*/
		SELECT DISTINCT
		sls_sales,
		sls_quantity,
		sls_price
		FROM bronze.crm_sales_details
		WHERE sls_sales != sls_quantity * sls_price								-- Checking consistency with Business Rules
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL		-- Checking if there are Null values
		OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0				-- Checking if there zeros or negative values
		ORDER BY sls_sales, sls_quantity, sls_price
		
--- Write the query script that will clean/transform the data
/*The result of the query shows that the quality of the sales and price data is wrong.
The solution is here to speak with the source system experts to directly fix the data issues from the source system. 
The alternative will be to improve the quality of the data in the Data Warehouse but you will need the support of the source experts
because it really depends on their rules as different rules makes different transformation.
Assuming we have this rules:
       ----If Sales is negative, zero or null, derive it using Quantity and Price
	   ----If Price is zero or null, calculate it using Sales and Quantity
	    ----If Price is negative, convert it to a positive value
The ABS function will convert the price from negative to a positive
*/
		SELECT DISTINCT
		sls_sales AS old_sls_sales,
		sls_quantity,
		sls_price AS old_sls_price,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)  -- Use the ABS function to return the absolute value of a number
			 THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			 THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details
		WHERE sls_sales != sls_quantity * sls_price								
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL		
		OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0				
		ORDER BY sls_sales, sls_quantity, sls_price


/* 
Finally, write the INSERT INTO Statement after you have identified and cleaned the data.
We have now inserted clean data inside the silver table.
*/
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)  -- Use the ABS function to return the absolute value of a number
			 THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			 THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details

/*
QUALITY CHECK FOR THE SILVER TABLE
Rerun the quality check queries from the bronze layer to verify the quality of data in the silver layer.
*/
   
  --->> CHECK FOR UNWANTED SPACES IN STRING VALUES
		SELECT 
		sls_ord_num
		FROM silver.crm_sales_details
		WHERE sls_ord_num != TRIM(sls_ord_num) --- Everything looks great. No unwanted spaces in this column. 

   --->> CHECK FOR INVALID DATE ORDERS
		SELECT *
		FROM silver.crm_sales_details
		WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

   --->> CHECK FOR NULLS OR NEGATIVE NUMBERS
		SELECT DISTINCT
		sls_sales,
		sls_quantity,
		sls_price
		FROM silver.crm_sales_details
		WHERE sls_sales != sls_quantity * sls_price								-- Checking consistency with Business Rules
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL		-- Checking if there are Null values
		OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0				-- Checking if there zeros or negative values
		ORDER BY sls_sales, sls_quantity, sls_price		 	   
			     
	   	  
---=======FINALLY CHECK THE ENTIRE SILVER TABLE=============
		SELECT *
		FROM silver.crm_sales_details
