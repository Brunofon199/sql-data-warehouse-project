-- Nos aseguramos que estamos en la base de datos correcta
USE DataWarehouse;



-- Nos aseguramos de no duplicar los datos 
TRUNCATE TABLE bronze.erp_cust_az12;
-- Insertamos nuestros datos
BULK INSERT bronze.erp_cust_az12
FROM '/var/opt/mssql/CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Verificamos la calidad de nuestros datos 
SELECT COUNT(*) FROM bronze.erp_cust_az12;

SELECT * FROM bronze.erp_cust_az12;
