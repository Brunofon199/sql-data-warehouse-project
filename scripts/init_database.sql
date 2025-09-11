-- Creamos la base de datos "DataWarehouse" 

USE master;

-- Eliminamos y recreamos la base de datos DataWarehouse

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK INMEDIATE;
  DROP DATABASE DataWarehouse;
END;

-- Creamos la base de datos 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

-- Nos cambiamos a DB DataWarehouse 
USE DataWarehouse;

-- Creamos los esquemas necesarias 
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
