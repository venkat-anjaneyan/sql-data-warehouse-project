--checking the foreign key joining
select * from gold.fact_sales sls
LEFT JOIN gold.dim_customers cu
ON sls.customer_key=cu.customer_key
LEFT JOIN gold.dim_products pds
on sls.product_key=pds.product_key
where cu.customer_key IS NULL
