/*
Create a Database and schemas

Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists. It is dropped and recreated. Additionally, the script sets up three schemas 
	within the database: 'bronze', 'silver', and 'gold'

*/

USE master;
GO

--- recreate database if exist
IF exists(select 1 from sys.databases where name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END
GO

--- create database
CREATE DATABASE DataWarehouse;
GO

--- switch to newly created database
USE DataWarehouse;
GO


-- create schema for all three layers
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
