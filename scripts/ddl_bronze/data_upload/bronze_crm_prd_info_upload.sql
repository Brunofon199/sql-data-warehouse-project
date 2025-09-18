-- Nos aseguramos que estamos en la base de datos correcta
USE DataWarehouse;



-- Nos aseguramos de no duplicar los datos 
TRUNCATE TABLE bronze.crm_prd_info;
-- Insertamos nuestros datos
BULK INSERT bronze.crm_prd_info
FROM '/var/opt/mssql/prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Verificamos la calidad de nuestros datos 
SELECT COUNT(*) FROM bronze.crm_prd_info;

SELECT * FROM bronze.crm_prd_info;
