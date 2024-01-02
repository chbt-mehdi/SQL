-- Use this table to 
-- compute order_binary for the 30 day window after the test_start_date
-- for the test named item_test_2


-- Get the count of distinct items and the sum of items ordered within 30 days for each test assignment
SELECT 
    order_data.test_assignment,
    COUNT(DISTINCT order_data.item_id) AS num_orders,
    SUM(order_data.orders_bin_30d) AS sum_orders_bin_30d
FROM 
(
    -- For each item, determine whether an order was placed within 30 days of the test start date
    SELECT 
        final_assignments.item_id,
        final_assignments.test_assignment,
        MAX(
            CASE
                WHEN (DATE (orders.created_at) - DATE (final_assignments.test_start_date)) BETWEEN 1 AND 30 THEN 1
                ELSE 0
            END
        ) AS orders_bin_30d
    FROM 
        dsv1069.final_assignments
    LEFT JOIN 
        dsv1069.orders AS orders ON final_assignments.item_id = orders.item_id
    WHERE 
        final_assignments.test_number = 'item_test_2'
    GROUP BY 
        final_assignments.item_id,
        final_assignments.test_assignment
) AS order_data
GROUP BY 
    order_data.test_assignment;
