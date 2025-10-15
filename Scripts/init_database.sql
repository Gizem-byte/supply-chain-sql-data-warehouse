/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'Supply_Chain_Datawarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'Supply_Chain_Datawarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


-- Drop and recreate the 'Supply_Chain_Datawarehouse' database


IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Supply_Chain_Datawarehouse')
BEGIN
    ALTER DATABASE Supply_Chain_Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Supply_Chain_Datawarehouse;
END;
GO

CREATE DATABASE Supply_Chain_Datawarehouse;
GO

USE Supply_Chain_Datawarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
