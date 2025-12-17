/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'DataWarhouse' database

IF EXISTS (SELECT 1 FROM sys.database WHERE name = 'DataWarhouse')
BEGIN
	ALTER DATABASE DataWarhose SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarhouse
END;

-- Create the 'DataWarhouse' Database

CREATE DATABASE DataWarhouse;

-- Use the Database

USE DATABASE DataWarhouse;

-- Create Schemas

CREATE SCHEMA bronze;

CREATE SCHEMA sliver;

CREATE SCHEMA gold;
