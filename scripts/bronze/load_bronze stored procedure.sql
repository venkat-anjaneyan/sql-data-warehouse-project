/*
====================================================
Stored Procedure: Load Bronze Layer to the System
====================================================
Script Purpose:
  This stored procedure is intended to simplify the task of loading the data from the source using bulk insert everyday.
  Initially, the table is trucncated to remove all the values from the table and uploads the data as full upload everytime.
  Additionally, start time and end time is captured for each tables and for the full batch to figure out the loading time
  and helps with optimisaiton.
  This stored procedure includes catch part to check the error if occurs.

Paramaters:
None. This stored procedure does not accept any parameters or returns any value.
============================================================
*/

create or alter procedure bronze.load_bronze 
as
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		print '=====================================';
		print 'Loading bronze data from the source';
		print '======================================';
		print 'table name:bronze.crm_cust_info';
		print '-----------------------------------';
		set @batch_start_time=GETDATE();

		--bulk inserting all the data into the table-crm-cust info	
		set @start_time=GETDATE();
		truncate table bronze.crm_cust_info;
		bulk insert bronze.crm_cust_info
		from 'C:\temp\source_crm\cust_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'

		--bulk inserting all the data into the table-crm-prd info
		print 'table name:bronze.crm_prd_info';
		print '-----------------------------------';
		set @start_time=GETDATE();
		truncate table bronze.crm_prd_info;

		bulk insert bronze.crm_prd_info
		from 'C:\temp\source_crm\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'

		--bulk inserting all the data into the table-crm-sales details
		print 'table name:bronze.crm_sales_details';
		print '-----------------------------------';
		set @start_time=GETDATE();
		truncate table bronze.crm_sales_details;

		bulk insert bronze.crm_sales_details
		from 'C:\temp\source_crm\sales_details.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'

		--bulk inserting all the data into the table-erp-cust az 12
		print 'table name:bronze.erp_cust_az12';
		print '-----------------------------------';
		set @start_time=GETDATE();
		truncate table bronze.erp_cust_az12;

		bulk insert bronze.erp_cust_az12
		from 'C:\temp\source_erp\CUST_AZ12.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'

		--bulk inserting all the data into the table-erp-loc_a101
		print 'table name:bronze.erp_loc_a101';
		print '-----------------------------------';
		set @start_time=GETDATE();
		truncate table bronze.erp_loc_a101;

		bulk insert bronze.erp_loc_a101
		from 'C:\temp\source_erp\LOC_A101.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'

		--bulk inserting all the data into the table-erp_px_cat_g1v2
		print 'table name:bronze.erp_px_cat_g1v2';
		print '-----------------------------------';
		set @start_time=GETDATE();
		truncate table bronze.erp_px_cat_g1v2;

		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\temp\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		set @end_time=GETDATE();
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------'

		set @batch_end_time=GETDATE();
		print 'loading time for the whole batch: '+ cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds'
	end try
	begin catch
		print '------------------------------------------';
		print 'ERROR OCCURED WHILE LOADING BRONZE LAYER'
		print '------------------------------------------';
		print 'error message' + error_message();
		print 'error message' + cast(error_number() as nvarchar);
	end catch
end

exec bronze.load_bronze
