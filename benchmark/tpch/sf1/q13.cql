MATCH (ord: ORDERS)
OPTIONAL MATCH (ord)-[:MADE_BY]->(customer: CUSTOMER)
WHERE NOT (ord.O_COMMENT =~ ".*express.*deposits.*")
WITH customer.C_CUSTKEY AS c_id, COUNT(ord.O_ORDERKEY) AS c_count 
RETURN c_count, COUNT(*) AS custdist
ORDER BY custdist DESC, c_count DESC;