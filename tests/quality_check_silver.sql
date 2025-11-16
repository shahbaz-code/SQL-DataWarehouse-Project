/*
THIS IS THE SCRIPT I WROTE WHILE PREPARING DATA FOR SILVER LAYER

>> WHAT I HAVE DID IN THIS SCRIPT
   - DATA CLEANING
   - DATA ENRICHMENT
   - DATA STANDERDISATION & NORMALIZATION
   - NULL HANDLING
   - DATA TRANSFORMATION TO SILVER
*/

-- ==============================
-- CHECKING FOR BRONZE LAYER
-- ==============================

-- -----------------
-- crm_cst_info
-- -----------------

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- 
SELECT 
*
FROM bronze.crm_cust_info
WHERE cst_id = 29466

-- Check for unwanted spaces
-- Expectation: no result

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT *
FROM bronze.crm_cust_info


-- ------------------------------
-- QUELITY CHECK FOR SILVER LAYER
-- ------------------------------

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- 
SELECT 
*
FROM silver.crm_cust_info
WHERE cst_id = 29466

-- Check for unwanted spaces
-- Expectation: no result

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

-- BRONZE: cst_martial_status
-- SILVER: cst_marital_status

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

SELECT *
FROM silver.crm_cust_info


-- ==============================
-- CHECKING FOR BRONZE LAYER
-- ==============================

-- -----------------
-- crm_prd_info
-- -----------------

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces
-- Expectation: no result

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- check any null value
-- EXPECTATION: no result
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for invalid Date Orders
SELECT
*
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT COUNT(*) FROM bronze.crm_prd_info

-- ------------------------------
-- QUELITY CHECK FOR SILVER LAYER
-- ------------------------------

SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- check any null value
-- EXPECTATION: no result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for invalid Date Orders
SELECT
*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT * FROM silver.crm_prd_info

-- ==============================
-- CHECKING FOR BRONZE LAYER
-- ==============================

-- -----------------
-- crm_sales_details
-- -----------------

-- Check for unwanted spaces
-- Expectation: no result

SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- check for invalid dates
-- EXPECTATION: no result
SELECT 
NULLIF(sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 or sls_order_dt is null
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20501230
OR sls_order_dt < 19001204

-- check for invalid order date
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt>sls_due_dt

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- check data consistency: between sales, Quentity, and Price
-- >> sales = Quentity * Price
-- >> Values must not be NULL, zero , or Negative.

SELECT DISTINCT
sls_sales as old_sales,
sls_quantity,
sls_price as old_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=  0 or sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * abs(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=  0
		THEN sls_sales/ nullif(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price


-- ------------------------------
-- QUELITY CHECK FOR SILVER LAYER
-- ------------------------------
-- check for invalid order date
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt>sls_due_dt

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price

SELECT * FROM silver.crm_sales_details

-- ERP SOURCE SYSTEM
-- -----------------

-- erp_CUST_AZ12
-- Identify Out-of_range Dates

SELECT DISTINCT
BDATE
FROM bronze.erp_CUST_AZ12
WHERE BDATE < '1924-01-01' OR BDATE > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT GEN,
CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END GEN
FROM bronze.erp_CUST_AZ12

-- ------------------------------
-- QUELITY CHECK FOR SILVER LAYER
-- ------------------------------
SELECT DISTINCT
BDATE
FROM silver.erp_CUST_AZ12
WHERE BDATE < '1924-01-01' OR BDATE > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT
CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END GEN
FROM silver.erp_CUST_AZ12

SELECT * FROM silver.erp_cust_az12

-- ==============================
-- CHECKING FOR BRONZE LAYER
-- ==============================

-- -----------------
-- erp_loc_a101
-- -----------------
SELECT 
REPLACE(CID,'-','') CID,
CNTRY
FROM bronze.erp_LOC_A101
WHERE REPLACE(CID,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- Data Standardization & Consistency
SELECT DISTINCT
-- REPLACE(CID,'-','') CID,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US','USA','United States') THEN 'USA'
	 WHEN TRIM(CNTRY) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE CNTRY
END AS CNTRY
FROM bronze.erp_LOC_A101
ORDER BY CNTRY

-- ------------------------------
-- QUELITY CHECK FOR SILVER LAYER
-- ------------------------------
SELECT DISTINCT
CNTRY
FROM silver.erp_LOC_A101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101

-- ==============================
-- CHECKING FOR BRONZE LAYER
-- ==============================

-- -----------------
-- erp_px_cat_g1v2
-- -----------------

-- CHECKING for DUPLICATES

SELECT ID,
COUNT(*)
FROM bronze.erp_PX_CAT_G1V2
GROUP BY ID
HAVING COUNT(*) > 1

-- cheking for unwanted spaces
SELECT * FROM bronze.erp_PX_CAT_G1V2
WHERE SUBCAT != TRIM(SUBCAT)
OR MAINTENANCE != TRIM(MAINTENANCE)

-- Data Standardization & Consistency
SELECT DISTINCT MAINTENANCE
FROM bronze.erp_PX_CAT_G1V2

SELECT * FROM silver.erp_px_cat_g1v2

