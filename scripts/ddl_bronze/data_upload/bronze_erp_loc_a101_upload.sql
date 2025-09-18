-- Nos aseguramos que estamos en la base de datos correcta
USE DataWarehouse;



-- Nos aseguramos de no duplicar los datos 
TRUNCATE TABLE bronze.erp_loc_a101;
-- Insertamos nuestros datos
BULK INSERT bronze.erp_loc_a101
FROM '/var/opt/mssql/LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Verificamos la calidad de nuestros datos 
SELECT COUNT(*) FROM bronze.erp_loc_a101;

SELECT * FROM bronze.erp_loc_a101;
