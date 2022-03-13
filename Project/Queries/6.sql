SELECT ps_partkey, ps_supplycost*ps_availqty FROM partsupp
ORDER BY ps_supplycost*ps_availqty DESC LIMIT 10;