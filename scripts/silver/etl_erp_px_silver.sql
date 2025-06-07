/*
CLEAN & LOAD EACH TABLE (erp_px_cat_g1v2)
1. Handle Invalid Values
*/
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2  --- Everything looks great


/*
2. Check for Unwanted spaces in string values.
*/
		SELECT *
		FROM bronze.erp_px_cat_g1v2
		WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) --- Everything looks great

/*
3. Standardization & Consistency.
*/
		SELECT DISTINCT
		cat
		FROM bronze.erp_px_cat_g1v2  --- Everything looks great

		SELECT DISTINCT
		subcat
		FROM bronze.erp_px_cat_g1v2  --- Everything looks great

		SELECT DISTINCT
		maintenance
		FROM bronze.erp_px_cat_g1v2  --- Everything looks great

/* 
Finally, write the INSERT INTO Statement after you have identified and cleaned the data.
We have now inserted clean data inside the silver table.
*/
		INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance		
		)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2 

---=======FINALLY CHECK THE ENTIRE SILVER TABLE=============
		SELECT *
		FROM silver.erp_px_cat_g1v2 
