-- Limpieza de tabla crm_cust_info
-- Se eliminan registros nulos y se estandarizan campos
-- Se mantiene el registro más reciente por cst_id

insert into silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
         ELSE 'n/a' END as cst_material_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'n/a' END as cst_gndr,
    cst_create_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;
