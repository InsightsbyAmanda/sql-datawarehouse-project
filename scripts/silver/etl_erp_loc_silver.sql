/*
CLEAN & LOAD EACH TABLE (erp_loc_a101)
1. Handle Invalid Values
*/
		SELECT 
		cid,
		cntry
		FROM bronze.erp_loc_a101

--- Write the query script that will clean/transform the data
		SELECT 
		REPLACE(cid, '-', '') cid,
		cntry
		FROM bronze.erp_loc_a101
		

		SELECT 
		REPLACE(cid, '-', '') cid,
		cntry
		FROM bronze.erp_loc_a101
		WHERE REPLACE(cid, '-', '') NOT IN
		(SELECT cst_key FROM silver.crm_cust_info)

/*
2. Standardization and Consistency 
*/
		SELECT DISTINCT
		cntry
		FROM bronze.erp_loc_a101
		ORDER BY cntry

--- Write the query script that will clean/transform the data
		SELECT 
		REPLACE(cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101

/* 
Finally, write the INSERT INTO Statement after you have identified and cleaned the data.
We have now inserted clean data inside the silver table.
*/
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry		
		)
		SELECT 
		REPLACE(cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101

/*
QUALITY CHECK FOR THE SILVER TABLE
Rerun the quality check queries from the bronze layer to verify the quality of data in the silver layer.
*/
  --->> HANDLE INVALID VALUE
		SELECT 
		cid,
		cntry
		FROM silver.erp_loc_a101

   --->> STANDARDIZATION AND CONSISTENCY
		SELECT DISTINCT
		cntry
		FROM silver.erp_loc_a101
		ORDER BY cntry    
	   	  
---=======FINALLY CHECK THE ENTIRE SILVER TABLE=============
		SELECT *
		FROM silver.erp_loc_a101
