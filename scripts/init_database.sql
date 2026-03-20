/*

Create Database and Schemas:
----------------------------------------

Script Purpose:
	This script is intended to create a new databse "datawarehouse" after checking if it already exists.
	Additionally, we have create three schemas-bronze, silver and gold	within the database

WARNING:
	Running this script drops the database if it already exists.
	Please take the necessary backup if needed before executing the below query.
*/

use master;
go

--drop database if already exists
if exists (select 1 from sys.databases where name='datawarehouse')
begin
	alter database datawarehouse set single_user with rollback immediate;
	drop database datawarehouse;
end;
go


-- create the database "datawarehouse"
create database datawarehouse;
go

use datawarehouse;
go

--create schemas-bronze, gold and silver
create schema bronze;
GO

create schema silver;
GO

create schema gold;
GO
