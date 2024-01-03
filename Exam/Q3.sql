-- Use this table to 
-- compute order_binary for the 30 day window after the test_start_date
-- for the test named item_test_2

-- Use CTEs (Common Table Expressions) for better readability and maintainability
WITH item_level AS (
  SELECT 
    fa.test_assignment,
    fa.item_id, 
    -- Use COALESCE to handle potential NULL values from the LEFT JOIN
    COALESCE(MAX(CASE WHEN orders.created_at > fa.test_start_date THEN 1 ELSE 0 END), 0) AS order_binary_30d
  FROM 
    dsv1069.final_assignments fa
  LEFT OUTER JOIN
    dsv1069.orders
  ON 
    fa.item_id = orders.item_id 
    AND orders.created_at >= fa.test_start_date
    AND DATE_PART('day', orders.created_at - fa.test_start_date ) <= 30
  WHERE 
    fa.test_number = 'item_test_2'
  GROUP BY
    fa.test_assignment,
    fa.item_id
)

SELECT
  test_assignment,
  COUNT(item_id) as items,
  SUM(order_binary_30d) AS ordered_items_30d
FROM
  item_level
GROUP BY test_assignment;


