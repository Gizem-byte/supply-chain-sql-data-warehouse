
/*
===============================================================================
INIT SCRIPT — SAFE NON-DESTRUCTIVE VERSION
===============================================================================
Purpose:
    • Create the database only if it does NOT already exist.
    • If the database exists, keep it (no drop or overwrite).
    • Always ensure schemas (bronze, silver, gold) exist.
    • Safe to run multiple times — never deletes or replaces data.
===============================================================================
*/


-- Create database if missing
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name ='Supply_Chain_Datawarehouse')
BEGIN
    PRINT 'Database not found — creating [Supply_Chain_Datawarehouse]...';
    CREATE DATABASE Supply_Chain_Datawarehouse;
    PRINT 'Database [Supply_Chain_Datawarehouse] created successfully.';
END
ELSE
BEGIN
    PRINT 'Database [Supply_Chain_Datawarehouse] already exists — keeping existing version.';
END
GO

-- Switch to the database
USE Supply_Chain_Datawarehouse;
GO

-- Ensure schemas exist (Bronze, Silver, Gold)
PRINT 'Checking required schemas...';

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

-- Confirmation
PRINT 'Safe initialization complete — no data altered or removed.';
GO
