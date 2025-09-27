-- Limpiamos nuestros datos en la tabla de erp_cust_az12

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
    trim(gender) as gender
from bronze.erp_cust_az12
