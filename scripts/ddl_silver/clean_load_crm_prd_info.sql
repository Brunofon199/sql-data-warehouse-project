-- Modificamos el DDL de nuestra tabla crm_prd_info

if object_id('silver.crm_prd_info', 'U') is not NULL
    drop table silver.crm_prd_info;
create table silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_crete_date DATETIME2 DEFAULT GETDATE()
);

print 'Truncamos los datos ...'
TRUNCATE TABLE silver.crm_prd_info;
print 'Insertamos datos ...'
-- Insertamos nuestros datos 
INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
select 
    prd_id,
    replace(substring(prd_key, 1, 5), '-', '_') as cat_id, -- Extraemos el ID de categoria
    substring(prd_key, 7, len(prd_key)) as prd_key, -- Extraemos la llave del producto
    prd_nm, 
    isnull(prd_cost, 0) as prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a' 
    END as prd_line, -- Mapeamos la linea de productos a valores descriptivos
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- Calculamos las fechas de fin como un dia menos de la fehca de inicio
from bronze.crm_prd_info; 



select * from silver.crm_prd_info;
