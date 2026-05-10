MATCH (c1:CUSTOMER)
WHERE c1.C_ACCTBAL > 0.00 and 
substring(c1.C_PHONE,0,2) IN ["12","32","16","29","11","14","27"]
WITH avg(c1.C_ACCTBAL) as avg_bal
MATCH (c2:CUSTOMER) 
WHERE
NOT EXISTS {
	MATCH (o:ORDERS)-[:MADE_BY]->(c2)
} and
substring(c2.C_PHONE,0,2) IN ["12","32","16","29","11","14","27"] and
c2.C_ACCTBAL > avg_bal
RETURN substring(c2.C_PHONE,0,2) as cntrycode, COUNT(*) as numcust, SUM(c2.C_ACCTBAL) as totacctbal 
ORDER BY cntrycode;