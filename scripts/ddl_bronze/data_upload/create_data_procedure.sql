CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 

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

END
