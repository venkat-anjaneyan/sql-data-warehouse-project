/*
=================================================================
Stored Procedure:Laod Silver Layer (Bronze to Silver)
=================================================================
Script Purpose:
	This stored procedure performs the ETL (Extract, Transform and Load) process to populate the silver schema tables
	from the bronze table
Actions Performed:
	-Truncate silver tables
	-Load silve tables after truncating

This stored procedure does not return any values or accepts any parameters
=================================================================
*/
CREATE OR ALTER PROCEDURE silver.silver_load
AS
DECLARE @batch_start_time datetime, @batch_end_time datetime, @start_time datetime, @end_time datetime;
BEGIN
	BEGIN TRY
		SET @batch_start_time=GETDATE();

		SET @start_time=GETDATE();
		PRINT '**Truncating Table: silver.crm_cust_info**';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '**Inserting Data into Table: silver.crm_cust_info**';

		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			csr_gndr,
			cst_created_date
			)

		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname, --data cleaning to remove unwanted spaces
		TRIM(cst_lastname) as cst_lastname, --data cleaning to remove unwanted spaces
		CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			ELSE 'n/a'
		END as cst_marital_status, --Normalise/Standardise values to a redable format
		CASE WHEN UPPER(TRIM(csr_gndr))='M' THEN 'Male'
			WHEN UPPER(TRIM(csr_gndr))='F' THEN 'Female'
			ELSE 'n/a'
		END as cst_gndr, --Normalise/Standardise values to a redable format
		cst_created_date
		FROM (
		SELECT 
		* ,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_created_date DESC) as flag_num
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		) a
		WHERE flag_num = 1; --select and inset only the most recent record for the customers by removing duplicates
		
		set @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'
		-------------------------------------------------------------------------
		SET @start_time=GETDATE();		

		PRINT '**Truncating Table: silver.crm_prd_info**';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '**Inserting Data into Table: silver.crm_prd_info**';

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
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id, --extract category id
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key, --extract product key
		prd_nm,
		COALESCE(prd_cost,0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Sport'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a' 
		END as prd_line, --data standardisaiton by making it more descriptive
		prd_start_dt,
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt --calculated end data using start date of the next row
		FROM bronze.crm_prd_info;

		SET @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'

		-------------------------------------------------------------------
		SET @start_time=GETDATE();		

		PRINT '**Truncating Table: silver.crm_sales_details**';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '**Inserting Data into Table: silver.crm_sales_details**';

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
		CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt, -- handling invalid date format
		CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt, -- handling invalid date format
		CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt, -- handling invalid date format
		CASE WHEN sls_sales IS NULL AND sls_price > 0 THEN sls_quantity*sls_price
			WHEN sls_sales <=0 AND sls_price > 0 THEN sls_quantity*sls_price
			ELSE sls_sales -- handling wrong data using recalculation from price and quantity 
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL AND sls_sales >0 THEN sls_sales/sls_quantity
			WHEN sls_sales >0 THEN sls_sales/sls_quantity
			ELSE sls_price
		END as sls_price -- handling wrong data using recalculation  
		FROM bronze.crm_sales_details;

		SET @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'
 
		 -------------------------------------------------------
		SET @start_time=GETDATE();		
		PRINT '**Truncating Table: silver.erp_cust_az12**';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '**Inserting Data into Table: silver.erp_cust_az12**';

		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)

		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,Len(cid)) 
			ELSE cid
		END AS cid, -- Rmeoved NAS prefix from the id
		CASE WHEN bdate <'1950-01-01' or bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate, --replaced old dates and fututre DOB into NULL
		CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			ELSE 'n/a' -- Normalised gender values from unknown and incorrect values
		END AS gen
		FROM bronze.erp_cust_az12;

		SET @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'

		-----------------------------------------------------------
		SET @start_time=GETDATE();		
		PRINT '**Truncating Table: silver.erp_loc_a101**';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '**Inserting Data into Table: silver.erp_loc_a101**';

		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)

		SELECT 
		REPLACE(cid,'-','') AS cid,
		CASE WHEN cntry IN ('US','USA','United States','US') THEN 'USA'
			WHEN cntry IN ('DE', 'Germany') THEN 'Germany'
			WHEN cntry IS NULL OR cntry='' THEN 'n/a'
			ELSE cntry
		END AS cntry --Normalised data for the countries
		FROM bronze.erp_loc_a101;
		
		SET @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'

		----------------------------------------------------------
		SET @start_time=GETDATE();
		PRINT '**Truncating Table: silver.erp_px_cat_g1v2**';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '**Inserting Data into Table: silver.erp_px_cat_g1v2**';

		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
		TRIM(id) as id,
		TRIM(cat) as cat,
		TRIM(subcat) as subcat,
		TRIM(maintenance) as maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'
		
		set @batch_end_time=GETDATE();
		print 'loading time for the whole batch: '+ cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds'

	END TRY
	BEGIN CATCH
		PRINT '------------------------------------------';
		PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER'
		PRINT '------------------------------------------';
		PRINT 'error message' + error_message();
		PRINT 'error message' + cast(error_number() as nvarchar);
	END CATCH
END
