-- Use this table to 
-- compute view_binary for the 30 day window after the test_start_date
-- for the test named item_test_2

-- Use CTEs (Common Table Expressions) for better readability and maintainability
WITH views AS (
  SELECT 
    event_time,
    event_id,
    CAST(parameter_value AS INT) AS item_id
  FROM 
    dsv1069.events 
  WHERE 
    event_name = 'view_item'
  AND 
    parameter_name = 'item_id'
), 

item_level AS (
  SELECT 
    fa.test_assignment,
    fa.item_id, 
    -- Use COALESCE to handle potential NULL values from the LEFT JOIN
    COALESCE(MAX(CASE WHEN views.event_time > fa.test_start_date THEN 1 ELSE 0 END), 0) AS view_binary_30d,
    COUNT(views.event_id) AS views
  FROM 
    dsv1069.final_assignments fa
  LEFT OUTER JOIN 
    views
  ON 
    fa.item_id = views.item_id
  AND 
    views.event_time >= fa.test_start_date
  AND 
    DATE_PART('day', views.event_time - fa.test_start_date ) <= 30
  WHERE 
    fa.test_number = 'item_test_2'
  GROUP BY
    fa.test_assignment,
    fa.item_id
)

SELECT
  test_assignment,
  COUNT(item_id) AS items,
  SUM(view_binary_30d) AS viewed_items,
  -- Use ROUND for better control over decimal places
  ROUND((100.0 * SUM(view_binary_30d)) / COUNT(item_id), 2) AS viewed_percent,
  SUM(views) AS views,
  -- Use COALESCE to handle potential division by zero
  COALESCE(SUM(views) / NULLIF(COUNT(item_id), 0), 0) AS average_views_per_item
FROM 
  item_level
GROUP BY 
  test_assignment;
