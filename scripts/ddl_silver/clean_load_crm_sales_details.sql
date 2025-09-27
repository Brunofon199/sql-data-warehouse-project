-- Modificamos el DLL de las tablas de sales_details
if object_id ('silver.crm_sales_details', 'U') IS NOT NULL  
    drop table silver.crm_sales_details;

create table silver.crm_sales_details(
    sls_old_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

print 'Truncamos los datos ...'
TRUNCATE TABLE silver.crm_sales_details;
print 'Insertamos datos ...'
--  Insertamos el conjunto de datos 

insert into silver.crm_sales_details (
    sls_old_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 or len(sls_order_dt) != 8 THEN NULL 
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 or len(sls_ship_dt) != 8 THEN NULL 
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 or len(sls_due_dt) != 8 THEN NULL 
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales 
    end as sls_sales, -- Recalculamos las ventas si el valor original is faltante o nulo
    sls_quantity,
    case when sls_price is null or sls_price <= 0
        then sls_sales / nullif(sls_quantity, 0)
        else sls_price
    end as sls_price
from bronze.crm_sales_details;


SELECT * from silver.crm_sales_details;
