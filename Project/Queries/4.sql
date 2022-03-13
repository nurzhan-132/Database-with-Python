SELECT COUNT(DISTINCT o_orderkey) FROM orders 
WHERE strftime('%Y', o_orderdate) = '1993';