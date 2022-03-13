SELECT p_type, COUNT(p_type) FROM part 
GROUP BY p_type HAVING p_type LIKE '%NICKEL%' ORDER BY p_type ASC;