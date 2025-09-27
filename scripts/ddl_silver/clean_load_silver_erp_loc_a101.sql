-- Insertamos datos limios a la tabla silver.erp_loc_a101
-- Limpiamos el campo cid y cntry
-- Eliminamos guiones del campo cid
insert into silver.erp_loc_a101(
    cid,
    cntry
)
SELECT 
replace(cid, '-', '') cid,
case when trim(cntry) = 'DE' then 'Germany'
    when trim(cntry) in ('USA', 'US') then 'United States'
    when trim(cntry) = '' or cntry is null then 'n/a'
    else trim(cntry)
end as cntry 
from bronze.erp_loc_a101;
