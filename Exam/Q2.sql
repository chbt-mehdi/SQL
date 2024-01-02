--Reformat the final_assignments_qa to look like the final_assignments table, filling in any missing values with a placeholder of the appropriate data type.

SELECT item_id,
       UNNEST(ARRAY[test_a, test_b, test_c, test_d, test_e, test_f]) AS test_assignment,
       UNNEST(ARRAY['test_a', 'test_b', 'test_c', 'test_d', 'test_e', 'test_f']) AS test_number,
       CAST('2024-01-02 00:00:00' AS timestamp) AS test_start_date
FROM dsv1069.final_assignments_qa


