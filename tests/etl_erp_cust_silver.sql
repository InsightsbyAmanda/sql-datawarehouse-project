/*
CLEAN & LOAD EACH TABLE (erp_cust_az12)
1. Handle Invalid Values
*/
		SELECT 
		cid,
		bdate,
		gen
		FROM bronze.erp_cust_az12

--- Write the query script that will clean/transform the data
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid,
		bdate,
		gen
		FROM bronze.erp_cust_az12

/*
CLEAN & LOAD EACH TABLE (erp_cust_az12)
2. Identify Out-of Range Dates 
*/
		SELECT DISTINCT
		bdate
		FROM bronze.erp_cust_az12
		WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--- Write the query script that will clean/transform the data
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		gen
		FROM bronze.erp_cust_az12

/*
CLEAN & LOAD EACH TABLE (erp_cust_az12)
3. Standardization and Consistency 
*/
		SELECT DISTINCT
		gen
		FROM bronze.erp_cust_az12

--- Write the query script that will clean/transform the data
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
			 ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12

/* 
Finally, write the INSERT INTO Statement after you have identified and cleaned the data.
We have now inserted clean data inside the silver table.
*/
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen		
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL					   -- Set future birthdates to NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
			 ELSE 'n/a'
		END AS gen													-- Normalize gender values and handle unknown cases.
		FROM bronze.erp_cust_az12

/*
QUALITY CHECK FOR THE SILVER TABLE
Rerun the quality check queries from the bronze layer to verify the quality of data in the silver layer.
*/
  --->> HANDLE INVALID VALUE
		SELECT 
		cid,
		bdate,
		gen
		FROM silver.erp_cust_az12

  --->> IDENTIFY OUT-OF-RANGE DATE
		SELECT DISTINCT
		bdate
		FROM silver.erp_cust_az12
		WHERE bdate < '1924-01-01' OR bdate > GETDATE()

   --->> STANDARDIZATION AND CONSISTENCY
		SELECT DISTINCT
		gen
		FROM silver.erp_cust_az12	     
	   	  
---=======FINALLY CHECK THE ENTIRE SILVER TABLE=============
		SELECT *
		FROM silver.erp_cust_az12
