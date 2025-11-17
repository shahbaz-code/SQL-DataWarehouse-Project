-- ===========================================
-- QUALITY CHECK FOR GOLD LAYER
-- ===========================================

-- --------------------------------
-- Quelity check for dim_customers
-- --------------------------------

SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen,'n/a')
	END new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER  BY 1,2;

SELECT DISTINCT gender FROM gold.dim_customers;

-- --------------------------------
-- Quelity check for dim_products
-- --------------------------------
SELECT * FROM gold.dim_products;

-- --------------------------------
-- Quelity check for fact_sales
-- --------------------------------

SELECT * FROM gold.fact_sales;

-- Foregn key integrity check (Dimentions)
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
