CREATE OR ALTER PROCEDURE silver.load_silver
AS -- Se agregó la palabra clave AS
BEGIN 
    -- Declaramos las variables para medir el tiempo de ejecución
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME;

    BEGIN TRY
        SET @start_batch_time = GETDATE();
        PRINT('======================================================');
        PRINT('Cargando y transformando datos en tablas Silver');
        PRINT('======================================================');

        ------------------------------------------------------------
        -- Tabla: silver.crm_cust_info
        ------------------------------------------------------------
        PRINT('------------------------------------------------------');
        PRINT('Procesando tabla: silver.crm_cust_info');
        PRINT('------------------------------------------------------');
        SET @start_time = GETDATE();

        PRINT('TRUNCATE TABLE silver.crm_cust_info;');
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT('INSERTANDO DATOS TRANSFORMADOS EN silver.crm_cust_info...');
        INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr, cst_create_date)
        SELECT cst_id, cst_key, trim(cst_firstname) as cst_firstname, trim(cst_lastname) as cst_lastname, 
               CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single' WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married' ELSE 'n/a' END as cst_material_status, 
               CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' ELSE 'n/a' END as cst_gndr, 
               cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        ------------------------------------------------------------
        -- Tabla: silver.crm_prd_info
        ------------------------------------------------------------
        PRINT('------------------------------------------------------');
        PRINT('Procesando tabla: silver.crm_prd_info');
        PRINT('------------------------------------------------------');
        SET @start_time = GETDATE();

        PRINT('TRUNCATE TABLE silver.crm_prd_info;');
        TRUNCATE TABLE silver.crm_prd_info;
        
        PRINT('INSERTANDO DATOS TRANSFORMADOS EN silver.crm_prd_info...');
        INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        SELECT prd_id, replace(substring(prd_key, 1, 5), '-', '_') as cat_id, substring(prd_key, 7, len(prd_key)) as prd_key, prd_nm, isnull(prd_cost, 0) as prd_cost,
               CASE UPPER(TRIM(prd_line)) WHEN 'M' THEN 'Mountain' WHEN 'R' THEN 'Road' WHEN 'S' THEN 'Sales' WHEN 'T' THEN 'Touring' ELSE 'n/a' END as prd_line,
               CAST (prd_start_dt AS DATE) AS prd_start_dt,
               CAST(LEAD(prd_start_dt, 1, '9999-12-31') OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        ------------------------------------------------------------
        -- Tabla: silver.crm_sales_details
        ------------------------------------------------------------
        PRINT('------------------------------------------------------');
        PRINT('Procesando tabla: silver.crm_sales_details');
        PRINT('------------------------------------------------------');
        SET @start_time = GETDATE();

        PRINT('TRUNCATE TABLE silver.crm_sales_details;');
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT('INSERTANDO DATOS TRANSFORMADOS EN silver.crm_sales_details...');
        INSERT INTO silver.crm_sales_details (sls_old_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        SELECT sls_ord_num, sls_prd_key, sls_cust_id,
               CASE WHEN sls_order_dt = 0 or len(sls_order_dt) != 8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
               CASE WHEN sls_ship_dt = 0 or len(sls_ship_dt) != 8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
               CASE WHEN sls_due_dt = 0 or len(sls_due_dt) != 8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt,
               CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) ELSE sls_sales END as sls_sales,
               sls_quantity,
               CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / nullif(sls_quantity, 0) ELSE sls_price END as sls_price
        FROM bronze.crm_sales_details; 

        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');
        
        ------------------------------------------------------------
        -- Tabla: silver.erp_cust_az12
        ------------------------------------------------------------
        PRINT('------------------------------------------------------');
        PRINT('Procesando tabla: silver.erp_cust_az12');
        PRINT('------------------------------------------------------');
        SET @start_time = GETDATE();

        PRINT('TRUNCATE TABLE silver.erp_cust_az12;');
        TRUNCATE TABLE silver.erp_cust_az12;
        
        PRINT('INSERTANDO DATOS TRANSFORMADOS EN silver.erp_cust_az12...');
        INSERT INTO silver.erp_cust_az12(cid, bdate, gender)
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 5, LEN(cid)) ELSE cid END as cid,
            CASE WHEN bdate < '1924-01-01' OR bdate > GETDATE() THEN NULL ELSE bdate END as bdate,
            CASE UPPER(TRIM(gender)) WHEN 'F' THEN 'Female' WHEN 'FEMALE' THEN 'Female' WHEN 'M' THEN 'Male' WHEN 'MALE' THEN 'Male' ELSE 'n/a' END as gender
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        ------------------------------------------------------------
        -- Tabla: silver.erp_loc_a101
        ------------------------------------------------------------
        PRINT('------------------------------------------------------');
        PRINT('Procesando tabla: silver.erp_loc_a101'); -- Corregido de crm_loc_a101
        PRINT('------------------------------------------------------');
        SET @start_time = GETDATE();

        PRINT('TRUNCATE TABLE silver.erp_loc_a101;'); -- Corregido de crm_loc_a101
        TRUNCATE TABLE silver.erp_loc_a101; -- Corregido de crm_loc_a101

        PRINT('INSERTANDO DATOS TRANSFORMADOS EN silver.erp_loc_a101...');
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT replace(cid, '-', '') cid,
               CASE WHEN trim(cntry) = 'DE' THEN 'Germany' WHEN trim(cntry) IN ('USA', 'US') THEN 'United States' WHEN trim(cntry) = '' or cntry IS NULL THEN 'n/a' ELSE trim(cntry) END as cntry 
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        ------------------------------------------------------------
        -- Tabla: silver.erp_px_cat_g1v2
        ------------------------------------------------------------
        PRINT('------------------------------------------------------');
        PRINT('Procesando tabla: silver.erp_px_cat_g1v2');
        PRINT('------------------------------------------------------');
        SET @start_time = GETDATE();

        PRINT('TRUNCATE TABLE silver.erp_px_cat_g1v2;'); -- CORREGIDO: Apuntaba a crm_loc_a101
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT('INSERTANDO DATOS TRANSFORMADOS EN silver.erp_px_cat_g1v2...');
        INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenace)
        SELECT id, cat, subcat, maintenace
        FROM bronze.erp_px_cat_g1v2;
        
        SET @end_time = GETDATE();
        PRINT('TIEMPO DE CARGA: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' segundos');

        -- Marcamos el tiempo final del batch completo
        SET @end_batch_time = GETDATE();
        PRINT('======================================================');
        PRINT('TIEMPO TOTAL DE CARGA EN TABLAS SILVER: ' + CAST(DATEDIFF(second, @start_batch_time, @end_batch_time) AS VARCHAR) + ' segundos');
        PRINT('======================================================');

    END TRY 
    BEGIN CATCH
        PRINT('============================================');
        PRINT('Error al cargar datos en tablas Silver');
        PRINT('============================================');
        -- Imprime información detallada del error
        PRINT('Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR));
        PRINT('Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR));
        PRINT('Error State: ' + CAST(ERROR_STATE() AS VARCHAR));
        PRINT('Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A'));
        PRINT('Error Line: ' + CAST(ERROR_LINE() AS VARCHAR));
        PRINT('Error Message: ' + ERROR_MESSAGE());
    END CATCH
END;
