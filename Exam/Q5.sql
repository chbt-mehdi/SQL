--Use the https://thumbtack.github.io/abba/demo/abba.html to compute the lifts in metrics and the p-values for the binary metrics ( 30 day order binary and 30 day view binary) using a interval 95% confidence. 

--Use the https://thumbtack.github.io/abba/demo/abba.html to compute the lifts in metrics and the p-values for the binary metrics ( 30 day order binary and 30 day view binary) using a interval 95% confidence. 

-- anwser:
--For views_item:  lift is and pval is 9.4% – 16% (13%) and pval is 0.0001
--Therefore for item_test_2, 'not statistically significant' there was no significant difference in either the number of views or the number of orders between control and experiment


-- Use CTEs (Common Table Expressions) for better readability and maintainability
WITH views AS (
  SELECT 
    event_time,
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
    COALESCE(MAX(CASE WHEN views.event_time > fa.test_start_date THEN 1 ELSE 0 END), 0) AS view_binary_30d
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
  SUM(view_binary_30d) AS viewed_items
FROM 
  item_level
GROUP BY 
  test_assignment;

