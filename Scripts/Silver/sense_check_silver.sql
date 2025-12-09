/*=====================================================================
   SILVER SENSE CHECK (Business Logic Validation Only)
   Table: silver.dataco_supply_chain

   PURPOSE:
     • Validate that cleaned & transformed Silver data is *business-sensible*
     • No structural checks (NULLs, patterns, typos, data types)
     • If structural: “Skipped — covered in DQA”
     • Directly aligned with transformations from proc_load_silver
=====================================================================*/

USE Supply_Chain_Datawarehouse;
GO

PRINT '===========================================================';
PRINT ' STARTING SILVER SENSE CHECK (Business Logic Only)';
PRINT '===========================================================';


/**********************************************************************
 0. High-Level Row & Key Sanity
**********************************************************************/
PRINT '--- 0. Basic Row Sanity ---';

SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders,
    COUNT(DISTINCT order_item_id) AS distinct_order_items,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    COUNT(DISTINCT product_barcode_id) AS distinct_products
FROM silver.dataco_supply_chain;



/**********************************************************************
 1. payment_type
  • Structural, not business → SKIPPED
**********************************************************************/
PRINT '--- 1. payment_type (Skipped – structural only in DQA) ---';



/**********************************************************************
 2–3. actual_shipping_days / scheduled_delivery_days
     FROM Bronze Checks: Shipping days should be realistic
     BUSINESS EXPECTATION:
       - actual >= 0
       - scheduled >= 0
       - actual ≤ 90 days (very long deliveries suspicious)
       - scheduled ≤ 60 days (very long promised times suspicious)
       - logical: actual_shipping_days ≥ scheduled_delivery_days should trigger lateness
**********************************************************************/
PRINT '--- 2–3. Shipping Days (Business Sanity) ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE actual_shipping_days < 0
   OR scheduled_shipping_days < 0
   OR actual_shipping_days > 90
   OR scheduled_shipping_days > 60;



/**********************************************************************
 4. earning_per_order_item
  • From Bronze: no negative profits except returns
  • Silver expectation: should be within realistic range
**********************************************************************/
PRINT '--- 4. earning_per_order_item (Business Sanity) ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE earning_per_order_item < -100
   OR earning_per_order_item > 2000;



/**********************************************************************
 5. total_sale_per_customer
  • From Bronze: cannot be negative
**********************************************************************/
PRINT '--- 5. total_sale_per_customer (Business Sanity) ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE total_sale_per_customer < 0
   OR total_sale_per_customer > 50000;



/**********************************************************************
 6. order_delivery_status
  • Structural → SKIPPED
**********************************************************************/
PRINT '--- 6. order_delivery_status (Skipped – structural only) ---';



/**********************************************************************
 7. order_late_delivery_risk
  • Bronze rule: if late, value must be 1
  • Silver logic: 
        actual > scheduled  → must be late
**********************************************************************/
PRINT '--- 7. order_late_delivery_risk (Business Logic) ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE actual_shipping_days > scheduled_shipping_days
  AND order_late_delivery_risk = 0;



/**********************************************************************
 8–9. product category fields
  • Purely descriptive → SKIPPED
**********************************************************************/
PRINT '--- 8–9. Product category fields (Skipped – structural only) ---';



/**********************************************************************
 10–17. Customer geographic fields
  • All structural (typos, nulls) handled in DQA
  • No business rules → SKIPPED
**********************************************************************/
PRINT '--- 10–17. Customer geography (Skipped – structural only) ---';



/**********************************************************************
 18–20. Address (street number, street name)
     BUSINESS SANITY:
        • street_number should be numeric
**********************************************************************/
PRINT '--- 18–20. Street Components (Business Logic) ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE TRY_CAST(customer_street_number AS INT) IS NULL
  AND customer_street_number IS NOT NULL;



/**********************************************************************
 21–22. Store department fields
     BUSINESS RULE:
        • department_id should not be absurdly large
**********************************************************************/
PRINT '--- 21–22. Department sanity (Business Logic) ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE store_department_id > 999;



/**********************************************************************
 23–24. Geo coordinates
     BUSINESS EXPECTATION (same as Bronze):
        • latitude MUST be between -90 and 90
        • longitude MUST be between -180 and 180
**********************************************************************/
PRINT '--- 23–24. Geo Coordinate sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE store_location_latitude NOT BETWEEN -90 AND 90
   OR store_location_longitude NOT BETWEEN -180 AND 180;



/**********************************************************************
 25–27. Market / Order geographies
     Structural only → SKIPPED
**********************************************************************/
PRINT '--- 25–27. Market & Order geography (Skipped – structural only) ---';



/**********************************************************************
 28. order_customer_id
     BUSINESS RULE:
       • Customer exists in Customer Id column? (referential check)
**********************************************************************/
PRINT '--- 28. Order Customer FK sanity ---';

SELECT *
FROM silver.dataco_supply_chain s
WHERE s.order_customer_id NOT IN (
    SELECT DISTINCT customer_id FROM silver.dataco_supply_chain
);



/**********************************************************************
 29–30. order_date / order_time
     BUSINESS RULES:
        • order_date should NOT be in future
**********************************************************************/
PRINT '--- 29–30. Order Date sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE order_date > CAST(GETDATE() AS DATE);



/**********************************************************************
 31. order_item_product_barcode_id
     BUSINESS EXPECTATION:
        • Must be a valid positive value
**********************************************************************/
PRINT '--- 31. Product Barcode sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE order_item_product_barcode_id <= 0;



/**********************************************************************
 32–33. discount fields
     BUSINESS RULES (from Bronze):
        • discount >= 0
        • discount_percentage between 0 and 1.5
**********************************************************************/
PRINT '--- 32–33. Discount sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE order_item_discount < 0
   OR order_item_discount_percentage < 0
   OR order_item_discount_percentage > 1.5;



/**********************************************************************
 34. order_item_id
     BUSINESS RULE:
       • No sense-check (line item key) → SKIPPED
**********************************************************************/
PRINT '--- 34. order_item_id (Skipped – no business rules) ---';



/**********************************************************************
 35–36. product price & profit ratio
     BUSINESS RULES:
       • price > 0
       • profit ratio between -5 and 10
**********************************************************************/
PRINT '--- 35–36. Unit Price & Profit Ratio sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE product_unit_price <= 0
   OR order_item_profit_ratio < -5
   OR order_item_profit_ratio > 10;



/**********************************************************************
 37. order_item_quantity
     BUSINESS RULE:
          • quantity must be >= 1
**********************************************************************/
PRINT '--- 37. Quantity sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE order_item_quantity < 1;



/**********************************************************************
 38–39. Gross & Net totals
     BUSINESS RULE:
       • NET cannot exceed GROSS
**********************************************************************/
PRINT '--- 38–39. Gross/Net sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE order_item_net_total > order_item_gross_total;



/**********************************************************************
 40. order_profit_per_order_item
     BUSINESS RULE:
       • Should be within reasonable business range
**********************************************************************/
PRINT '--- 40. Profit sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE order_profit_per_order_item < -500
   OR order_profit_per_order_item > 5000;



/**********************************************************************
 41–43. Order attributes (region/state/status)
     No business rules, descriptive only → SKIPPED
**********************************************************************/
PRINT '--- 41–43. Order attributes (Skipped – structural only) ---';



/**********************************************************************
 44. order_zipcode
     BUSINESS RULE:
        • zipcode must be > 0
**********************************************************************/
PRINT '--- 44. Zipcode sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE order_zipcode <= 0;



/**********************************************************************
 45–50. Product fields
     Already handled in price/profit checks → SKIPPED
**********************************************************************/
PRINT '--- 45–50. Product fields (Skipped – no extra business rules) ---';



/**********************************************************************
 51. product_status  
     BUSINESS LOGIC:
        • If exists: must be 0/1/2 range
**********************************************************************/
PRINT '--- 51. Product Status sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE product_status NOT IN (0,1,2);



/**********************************************************************
 52–53. shipping_date / shipping_time / shipping_mode
     BUSINESS RULES:
        • shipping_date >= order_date
**********************************************************************/
PRINT '--- 52–53. Shipping logic sanity ---';

SELECT *
FROM silver.dataco_supply_chain
WHERE shipping_date < order_date;



PRINT '===========================================================';
PRINT ' SILVER SENSE CHECK COMPLETED';
PRINT '===========================================================';
GO
