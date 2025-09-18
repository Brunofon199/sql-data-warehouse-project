-- Nos aseguramos que estamos en la base de datos correcta
USE DataWarehouse;



-- Nos aseguramos de no duplicar los datos 
TRUNCATE TABLE bronze.crm_sales_details;
-- Insertamos nuestros datos
BULK INSERT bronze.crm_sales_details
FROM '/var/opt/mssql/sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Verificamos la calidad de nuestros datos 
SELECT COUNT(*) FROM bronze.crm_sales_details;

SELECT * FROM bronze.crm_sales_details;
