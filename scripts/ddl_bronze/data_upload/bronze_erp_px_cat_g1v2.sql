-- Nos aseguramos que estamos en la base de datos correcta
USE DataWarehouse;



-- Nos aseguramos de no duplicar los datos 
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
-- Insertamos nuestros datos
BULK INSERT bronze.erp_px_cat_g1v2
FROM '/var/opt/mssql/PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

-- Verificamos la calidad de nuestros datos 
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

SELECT * FROM bronze.erp_px_cat_g1v2;
