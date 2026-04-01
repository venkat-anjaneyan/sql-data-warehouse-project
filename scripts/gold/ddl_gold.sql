/*
================================================================
DDL Script Purpose
================================================================
Script Purpose:
  This script creates views for gold layer:
  including dimension tables for customers and products,
  fact table for sales.
  This layer follows Star Schema DB

  Each view performs transformation using silver layer to produce gold layer for business needs.

*/


--- creating view for customers dimension table
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY	cst_id) AS customer_key,
	cin.cst_id AS csutomer_id,
	cin.cst_key AS customer_number,
	cin.cst_firstname AS first_name,
	cin.cst_lastname AS last_name,
	loc.cntry AS country,
	cin.cst_marital_status AS marital_status,
	CASE WHEN cin.csr_gndr!='n/a' THEN cin.csr_gndr
		ELSE COALESCE(cin2.gen, 'n/a')
	END AS gender,-- here considering crm_cust_info as master table and replacing gender wherever not available
	cin2.bdate AS birth_date,
	cin.cst_created_date AS create_date
FROM silver.crm_cust_info cin
LEFT JOIN silver.erp_cust_az12 cin2 -- using left join with the master table
ON cin.cst_key=cin2.cid
LEFT JOIN silver.erp_loc_a101 loc
ON cin.cst_key=loc.cid


 
-- create view for products dimension table
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cprd.prd_start_dt, cprd.prd_key) AS product_key, --surrogate key
	cprd.prd_id AS product_id,
	cprd.prd_key AS product_number,
	cprd.prd_nm AS product_name,
	cprd.cat_id AS category_id,
	eprd.cat AS category_name,
	eprd.subcat AS subcategory_name,
	cprd.prd_cost AS product_cost,
	cprd.prd_line AS product_line,
	cprd.prd_start_dt AS product_start_date,
	eprd.maintenance AS maintenance_required
FROM (
		SELECT *, ROW_NUMBER() OVER(PARTITION BY cprd.prd_key ORDER BY cprd.prd_start_dt DESC) as rn
		FROM silver.crm_prd_info cprd
	) cprd
LEFT JOIN silver.erp_px_cat_g1v2 eprd
ON cprd.cat_id=eprd.id
WHERE rn=1 --filtering out all historic data

--- create view for sales fact table
CREATE VIEW gold.fact_sales AS
SELECT
	sl.sls_ord_num AS order_number,
	cu.customer_key, --dimension key from customer table
	pd.product_key, --dimension key form products table
	sl.sls_order_dt AS order_date,
	sl.sls_ship_dt AS order_shippping_date,
	sl.sls_due_dt AS order_due_date,
	sl.sls_sales AS total_sales_amount,
	sl.sls_quantity AS order_quantity,
	sl.sls_price AS unit_price
FROM silver.crm_sales_details sl
LEFT JOIN gold.dim_products pd
ON sl.sls_prd_key=pd.product_number
LEFT JOIN gold.dim_customers cu
ON sl.sls_cust_id=cu.csutomer_id

