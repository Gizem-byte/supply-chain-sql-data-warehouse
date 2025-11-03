/*
===============================================================================
INIT SCRIPT — RESET VERSION (DROP + RECREATE)
===============================================================================
Purpose:
    • Drops the existing Supply_Chain_Datawarehouse database if it exists.
    • Creates a fresh new version of the database.
    • Always (re)creates the schemas: bronze, silver, gold.
    • WARNING: This version is DESTRUCTIVE — all existing data will be lost.
===============================================================================
*/

-- Drop database if it already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'Supply_Chain_Datawarehouse')
BEGIN
    PRINT 'Database [Supply_Chain_Datawarehouse] already exists — dropping and recreating...';
    ALTER DATABASE Supply_Chain_Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Supply_Chain_Datawarehouse;
    PRINT 'Existing database dropped successfully.';
END
ELSE
    PRINT 'Database not found — creating a fresh one.';
GO

-- Create new database
CREATE DATABASE Supply_Chain_Datawarehouse;
PRINT 'New database [Supply_Chain_Datawarehouse] created successfully.';
GO

-- Switch to the new database
USE Supply_Chain_Datawarehouse;
GO

-- Create schemas for Medallion architecture
PRINT 'Creating required schemas...';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze;');
    PRINT 'Created schema: bronze';
END
ELSE
    PRINT 'Schema bronze already exists.';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver;');
    PRINT 'Created schema: silver';
END
ELSE
    PRINT 'Schema silver already exists.';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold;');
    PRINT 'Created schema: gold';
END
ELSE
    PRINT 'Schema gold already exists.';
GO

-- 5Final confirmation
PRINT 'Database reset completed successfully.';
PRINT '----------------------------------------------';
PRINT 'Database : Supply_Chain_Datawarehouse';
PRINT 'Schemas  : bronze, silver, gold';
PRINT 'Status   : Freshly recreated (all old data removed)';
PRINT '----------------------------------------------';
GO
