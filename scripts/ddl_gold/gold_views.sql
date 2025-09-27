-- =============================================================================
-- Creamos la dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;

create view gold.dim_customer as
select 
    row_number() over (order by ci.cst_id) as customer_surrogate_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_source_key,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    ci.cst_material_status as material_status,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr
        else coalesce(ca.gender, 'n/a')
    end as gender,
    ci.cst_create_date as create_date,
    ca.bdate as birth_date,
    ca.gender as ca_gender_az12,
    la.cntry as country
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on      ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on      ci.cst_key = la.cid

-- =============================================================================
-- Creamos la dimension: gold.dim_products
-- =============================================================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
  DROP VIEW gold.dim_products;

create view gold.dim_products as
select 
  row_number() over (order by pn.prd_id) as product_surrogate_key,
  pn.prd_id as product_id,
  pn.cat_id as category_id,
  pn.prd_key as product_key,
  pn.prd_nm as product_name,
  pn.prd_cost as product_cost,
  pn.prd_line as product_line,
  pn.prd_start_dt as product_start_date,
  pn.prd_end_dt as product_end_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
  on pn.cat_id = pc.id
where pn.prd_end_dt is null


-- =============================================================================
-- Creamos la tabla de hechos: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;


create view gold.fact_sales as
select 
  sd.sls_old_num,
  pr.product_key,
  cu.customer_source_key,
  sd.sls_order_dt as order_date,
  sd.sls_ship_dt as ship_date,
  sd.sls_due_dt as due_date,
  sd.sls_sales as sales,
  sd.sls_quantity as quantity,
  sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
  on sd.sls_prd_key = pr.product_key
left join gold.dim_customer cu
  on sd.sls_cust_id = cu.customer_id
