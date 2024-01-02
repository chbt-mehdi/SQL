-- Use this table to 
-- compute view_binary for the 30 day window after the test_start_date
-- for the test named item_test_2


-- Get the count of distinct items, the sum of items viewed within 30 days, and the average of items viewed within 30 days for each test assignment
SELECT 
    view_data.test_assignment,
    COUNT(DISTINCT view_data.item_id) AS num_views,
    SUM(view_data.view_bin_30d) AS sum_view_bin_30d,
    AVG(view_data.view_bin_30d) AS avg_view_bin_30d
FROM 
(
    -- For each item, determine whether a view event occurred within 30 days of the test start date
    SELECT 
        assignments.item_id,
        assignments.test_assignment,
        MAX(
            CASE
                WHEN (DATE(views.event_time) - DATE(assignments.test_start_date)) BETWEEN 1 AND 30 THEN 1
                ELSE 0
            END
        ) AS view_bin_30d
    FROM 
        dsv1069.final_assignments AS assignments
    LEFT JOIN 
        dsv1069.view_item_events AS views ON assignments.item_id = views.item_id
    WHERE 
        assignments.test_number = 'item_test_2'
    GROUP BY 
        assignments.item_id,
        assignments.test_assignment
    ORDER BY 
        item_id
) AS view_data
GROUP BY 
    view_data.test_assignment;
