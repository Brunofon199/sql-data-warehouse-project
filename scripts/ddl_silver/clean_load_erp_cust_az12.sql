-- Limpiamos nuestros datos en la tabla de erp_cust_az12
print 'Truncamos los datos ...'
TRUNCATE TABLE silver.erp_cust_az12;
print 'Insertamos datos ...'
insert into silver.erp_cust_az12(
    cid,
    bdate,
    gender
)
select 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 5, LEN(cid))
        ELSE cid 
    end as cid, -- Eliminamos prefijos en el ID
    CASE WHEN bdate < '1924-01-01' OR bdate > GETDATE() THEN NULL
        ELSE bdate 
    end as bdate, -- Colocamos cumpleaños futuros como nulos 
    CASE UPPER(TRIM(gender)) -- Cambiamos siglas por lenguaje conocido
        WHEN 'F' THEN 'Female'
        when 'FEMALE' THEN 'Female'
        WHEN 'M' THEN 'Male'
        WHEN 'MALE' then 'Male'
        else 'n/a'
    end as gender,
from bronze.erp_cust_az12;
