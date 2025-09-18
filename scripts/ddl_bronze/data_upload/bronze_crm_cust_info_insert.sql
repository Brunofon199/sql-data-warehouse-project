-- Nos aseguramos que estamos en la base de datos correcta
USE DataWarehouse;

-- Nos aseguramos de no duplicar los datos 
TRUNCATE TABLE bronze.crm_cust_info;
-- Insertamos nuestros datos
BULK INSERT bronze.crm_cust_info
FROM '/var/opt/mssql/cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Verificamos la calidad de nuestros datos 
SELECT COUNT(*) FROM bronze.crm_cust_info;
