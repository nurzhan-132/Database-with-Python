SELECT s_name, s_acctbal FROM supplier 
WHERE s_acctbal = (SELECT MAX(s_acctbal) FROM supplier);