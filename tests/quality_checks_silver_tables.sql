/*
=====================================================================
Quality Checks
=====================================================================
Script Purpose:
  This script checks the quality of bronze tables before transormation or
  loading into silver tables.
  These queries checks the data cleansing including null values check, duplicates removal, unwanted spaces in text,
  correct datatypes, etc.,
  and data transfromation include data enrichment, integration, derived columns, data normalisaition & standarisaiton.
=========================================================================
*/

-------------------------------CRM_CST------------------------------
-- check for null or duplicate values in primary key

SELECT cst_id, COUNT(*)
from silver.crm_cust_info 
group by cst_id
having COUNT(*) >1 or cst_id is null;

--check for unwanted spaces
--Expectation: No results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname!=TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname!=TRIM(cst_lastname)

SELECT csr_gndr
FROM silver.crm_cust_info
WHERE csr_gndr!=TRIM(csr_gndr)

--Data standardization and consistency checking 
SELECT distinct cst_marital_status
FROM silver.crm_cust_info

SELECT distinct csr_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_prd_info

---------------------CRM_PRD--------------------------------------------

--To check null or duplicate values in primary key column
SELECT
prd_id, COUNT(*)
FROM
silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL

--check for unwanted spaces
SELECT
prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm!=TRIM(prd_nm)

--check for null or negative numbers
SELECT
prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost <0

--check for data standardisation and consistency
SELECT
distinct prd_line 
FROM silver.crm_prd_info

--check for Invalid date orders
SELECT
*
FROM silver.crm_prd_info
WHERE prd_end_dt IS NULL and prd_start_dt is null


-----------------------------------CRM_SALES--------------------------------
--To check null values in column
SELECT
*
FROM
silver.crm_sales_details
WHERE sls_ord_num is null

SELECT
*
FROM
silver.crm_sales_details
WHERE sls_cust_id is null or sls_ship_dt is null
or sls_due_dt is null or sls_sales is null
or sls_quantity is null or sls_price is null

--checking whether product keys are matching with prd key from prd_info table
select sls_prd_key
from silver.crm_sales_details
where sls_prd_key not in (
select prd_key from silver.crm_prd_info)

select sls_cust_id
from silver.crm_sales_details
where sls_cust_id not in (
select cst_id from silver.crm_cust_info)

--checking for invalid dates. Here we have dates in int, in this case, if the int is 0 or negative, date conversion will not happen.
select 
nullif(sls_ship_dt,0) sls_ship_dt
from silver.crm_sales_details
where sls_ship_dt<=0
or LEN(sls_ship_dt)!=8
or sls_ship_dt<19000101
or sls_ship_dt>20250101

--checking order date is greater than ship or due date
select * from silver.crm_sales_details where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt

select * from silver.crm_sales_details where sls_ship_dt>sls_due_dt

--checking sales price, quantity and sales columns for null, negative or sales !=quantity * price
select sls_sales, sls_quantity, sls_price
from silver.crm_sales_details
where sls_sales!=sls_quantity*sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <0 or sls_quantity <0 or sls_price <0
order by sls_price

select sls_sales, sls_quantity, sls_price
from silver.crm_sales_details where sls_sales <=0 and sls_price>0

-----------------------ERP-CUST-AZ12---------------------------------
SELECT cid, COUNT(*)
from silver.erp_cust_az12 
group by cid
having COUNT(*) >1 or cid is null;

--check for unwanted spaces
--Expectation: No results
SELECT cid
from silver.erp_cust_az12 
where cid!=TRIM(cid);

select bdate
from silver.erp_cust_az12 
where bdate <'1900-01-01'
or bdate >'2020-01-01'

select bdate
from silver.erp_cust_az12 
where LEN(bdate)!=10

select distinct gen
from silver.erp_cust_az12 

------------------------ERP-LOC-------------
select cid
from silver.erp_loc_a101
where LEN(cid)!=11

select distinct cntry
from silver.erp_loc_a101

------------------------ERP-PX-CAT-------------
select id
from silver.erp_px_cat_g1v2
where id NOT IN (
select cat_id from silver.crm_prd_info)

select distinct maintenance
from silver.erp_px_cat_g1v2
