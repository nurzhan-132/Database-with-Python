SELECT l_orderkey, MAX(l_extendedprice*(1-l_discount)) FROM lineitem
WHERE strftime('%Y %m %d', l_shipdate) <> '1994 11 28'
ORDER BY l_orderkey ASC;