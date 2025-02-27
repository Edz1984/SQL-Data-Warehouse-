/*
=====================
Create Database and Schemas
=====================
Script Purpose:
  This script creates a new database named 'DataWarehouse'.
  Additionally, the script sets up three schemas within the datase:
  'bronze', 'silver', and 'gold'.

Reminder to add code to dropped DB if it exist and recreate...

*/

-- Create Database 'DataWarehouse'

USE master;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO
  
Use DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
