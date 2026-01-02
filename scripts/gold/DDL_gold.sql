/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

DROP VIEW IF EXISTS dim_customer;
CREATE  VIEW gold.dim_customer AS 
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- surroget key
cci.cst_id AS customer_id,
cci.cst_key AS customer_number,
cci.cst_firstname AS first_name,
cci.cst_lastname AS last_name,
ela.cntry AS country,
cci.cst_marital_status As marital_status,
CASE 
	WHEN cci.cst_gndr != 'Unknown' THEN cci.cst_gndr -- CRM is Master for gender Info 
	ELSE COALESCE(eca.gen ,'Unknown') -- Fallback to ERP data
END AS gender,
eca.bdate AS birthdate,
cci.cst_create_date AS create_date
FROM sliver.crm_cust_info cci
LEFT JOIN sliver.erp_cust_az12 eca ON cci.cst_key = eca.cid
LEFT JOIN sliver.erp_loc_a101 ela ON cci.cst_key  = ela.cid;


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

DROP VIEW IF EXISTS dim_product;
CREATE  VIEW gold.dim_product AS 
SELECT 
  ROW_NUMBER() OVER (ORDER BY  pi.prd_start_dt, pi.prd_key) AS product_key,
  pi.prd_id AS product_id,
  pi.prd_key AS product_number,
  pi.prd_nm AS product_name,
  pi.cat_id AS category_id,
  pc.cat AS category,
  pc.subcat AS subcategory,
  pc.maintenance,
  pi.prd_cost AS product_cost,
  pi.prd_line AS product_line,
  pi.prd_start_dt AS start_date,
  pi.prd_end_dt AS end_date
FROM sliver.crm_prd_info pi
LEFT JOIN sliver.erp_px_cat_g1v2 pc ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL ;  -- filter out history data

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

DROP VIEW IF EXISTS fact_sales;
CREATE  VIEW gold.fact_sales AS 
SELECT 
sd.sls_ord_num AS order_number,
dp.product_key,
dc.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_price AS price,
sd.sls_quantity AS quantity,
sd.sls_sales AS sales_amount
FROM  sliver.crm_sales_details sd 
LEFT JOIN gold.dim_product dp  ON sd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customer dc ON sd.sls_cus_id = dc.customer_id; 
