SELECT COUNT(DISTINCT p_name) FROM part
WHERE p_type LIKE '%STANDARD BURNISHED%' AND (p_size = 6 OR p_size = 23 OR p_size = 43);