CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME;
    BEGIN TRY
        SET @start_batch_time = GETDATE();
        PRINT('============================================');
        PRINT('Cargando datos en tablas Bronze');
        PRINT('============================================');

    
        PRINT('---------------------------------------------');
        PRINT('Cargando datos en tablas Bronze - crm_cust_info');
        PRINT('---------------------------------------------');

        -- Declaramos variable de tiempo
        SET @start_time = GETDATE();

        PRINT('TRUNCATE TABLE bronze.crm_cust_info;');
        -- Nos aseguramos de no duplicar los datos 
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT('INSERTANDO DATOS EN bronze.crm_cust_info');
        -- Insertamos nuestros datos
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();

        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        SET @start_time = GETDATE();
        PRINT('TRUNCAMOS LA TABLA bronze.crm_prd_info');
        -- Nos aseguramos de no duplicar los datos 
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT('INSERTANDO DATOS EN bronze.crm_prd_info');
        -- Insertamos nuestros datos
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        SET @start_time = GETDATE();
        PRINT('TRUNCAMOS LA TABLA bronze.crm_sales_details');
        -- Nos aseguramos de no duplicar los datos 
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT('INSERTANDO DATOS EN bronze.crm_sales_details');
        -- Insertamos nuestros datos
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        SET @start_time = GETDATE();
        PRINT('TRUNCAMOS LA TABLA bronze.erp_cust_az12');
        -- Nos aseguramos de no duplicar los datos 
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT('INSERTANDO DATOS EN bronze.erp_cust_az12');
        -- Insertamos nuestros datos
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        SET @start_time = GETDATE();
        PRINT('TRUNCAMOS LA TABLA bronze.erp_loc_a101');
        -- Nos aseguramos de no duplicar los datos 
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT('INSERTANDO DATOS EN bronze.erp_loc_a101');
        -- Insertamos nuestros datos
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        SET @start_time = GETDATE();
        PRINT('TRUNCAMOS LA TABLA bronze.erp_px_cat_g1v2');
        -- Nos aseguramos de no duplicar los datos 
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT('INSERTANDO DATOS EN bronze.erp_px_cat_g1v2');
        -- Insertamos nuestros datos
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        SET @end_batch_time = GETDATE();
        PRINT('TIEMPO TOTAL DE CARGA EN TABLAS BRONZE: ' + CAST(DATEDIFF(second, @start_batch_time, @end_batch_time) AS VARCHAR) + ' segundos');
    END TRY 
    BEGIN CATCH
        PRINT('============================================');
        PRINT('Error al cargar datos en tablas Bronze');
        PRINT('============================================');
        PRINT(ERROR_MESSAGE());
    END CATCH
END
