/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


Exec bronze.load_bronze

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime; -- Declaring the time variables to messaure the excecution time
	set @start_time = getdate(); -- Records the start time of the bronze layer excecution
	begin try
		print'=============================================';
		print'Loading Bronze Layer';
		print'=============================================';

		print'-----------------------------------------------';
		print'>> Loading CRM Table Data <<';
		print'-----------------------------------------------';

		set @start_time = getdate(); -- Records the below table execution start time 

		truncate table bronze.crm_cust_info; 
		-- it deletes the table data quickly and reloads the fresh data based on the below bulk insert so that it doesnt repeatduplicates
		-- this type is called full load
		bulk insert bronze.crm_cust_info
			from 'C:\Users\naren\OneDrive\Desktop\Data Warehouse Project\source_crm\cust_info.csv'
		with
			(
				firstrow = 2,
				fieldterminator = ',',
				tablock --this locks the entire table during loading
			);

		set @end_time = getdate(); -- Records the table execution end time
		print'>> Load Duration for the table is: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print'----------------------------------------';

		set @start_time = getdate(); --Records the below table execution start time
		-- Second table data insertion
		truncate table bronze.crm_prd_info; 
		bulk insert bronze.crm_prd_info
			from 'C:\Users\naren\OneDrive\Desktop\Data Warehouse Project\source_crm\prd_info.csv'
		with
			(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		set @end_time = getdate(); --Records the below table execution start time and same for other tables as well below
		print'>> Load Duration for the table is: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print'----------------------------------------';


		set @start_time = getdate();
		--third table data insertion
		truncate table bronze.crm_sales_details; 
		bulk insert bronze.crm_sales_details
			from 'C:\Users\naren\OneDrive\Desktop\Data Warehouse Project\source_crm\sales_details.csv'
		with
			(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);		
		set @end_time = getdate();
		print'>> Load Duration for the table is: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print'----------------------------------------';


		print'-----------------------------------------------';
		print'>> Loading CRM Table Data <<';
		print'-----------------------------------------------';
		--fourth table data insertion(ERP)

		set @start_time = getdate();
		truncate table bronze.erp_cust_az12; 
		bulk insert bronze.erp_cust_az12
			from 'C:\Users\naren\OneDrive\Desktop\Data Warehouse Project\source_erp\CUST_AZ12.csv'
		with
			(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		set @end_time = getdate();
		print'>> Load Duration for the table is: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print'----------------------------------------';

		--fourth table data insertion(ERP)
		set @start_time = getdate();
		truncate table bronze.erp_cust_az12; 
		bulk insert bronze.erp_cust_az12
			from 'C:\Users\naren\OneDrive\Desktop\Data Warehouse Project\source_erp\CUST_AZ12.csv'
		with
			(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		
		set @end_time = getdate();
		print'>> Load Duration for the table is: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print'----------------------------------------';


		--fifth table data insertion(ERP)
		set @start_time = getdate();
		truncate table bronze.erp_loc_a101; 
		bulk insert bronze.erp_loc_a101
			from 'C:\Users\naren\OneDrive\Desktop\Data Warehouse Project\source_erp\LOC_A101.csv'
		with
			(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
		set @end_time = getdate();
		print'>> Load Duration for the table is: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + ' ' + 'seconds';
		print'----------------------------------------';

		--six table data insertion(ERP)
		set @start_time = getdate();
		truncate table bronze.erp_px_cat_g1v2; 
		bulk insert bronze.erp_px_cat_g1v2
			from 'C:\Users\naren\OneDrive\Desktop\Data Warehouse Project\source_erp\PX_CAT_G1V2.csv'
		with
			(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);

		set @end_time = getdate();
		print'>> Load Duration for the table is: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';
		print'----------------------------------------';

	end try
	begin catch
		print'***************************************';
		print'Error Occured During Loading Bronze Layer';
		print'Error Message'+ error_message();
		print'Error Message'+ cast(error_number()as nvarchar);
		print'Error Message'+ cast(error_state()as nvarchar);
		print'***************************************';
	end catch
	set @end_time = getdate(); -- Records the end time of the bronze layer excecution
		print'>>the bronze layer execution is completed'
		print'>> Total Load Duration: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';
		print'----------------------------------------';
end
--select * from bronze.crm_cust_info;  --- we do this to check the data quality
--select count(*) from bronze.crm_cust_info --- we do this to check the data quality

