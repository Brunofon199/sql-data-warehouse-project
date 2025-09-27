-- Limpiamos y cargamos tabla silver.erp_cat_g1v2
-- Limpiamos espacios vacios en los campos cat, subcat y maintenace

insert into silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenace
)
select 
id,
cat,
subcat,
maintenace
from bronze.erp_px_cat_g1v2;

-- Revisamo falta de espacios vacios 
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenace != trim(maintenace);
