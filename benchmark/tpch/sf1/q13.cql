MATCH (customer:CUSTOMER)
OPTIONAL MATCH (customer)<-[:MADE_BY]-(ord:ORDERS)
WHERE NOT (ord.O_COMMENT =~ ".*express.*deposits.*")
WITH customer.C_CUSTKEY AS c_id, count(ord.O_ORDERKEY) AS c_count
RETURN c_count, count(*) AS custdist
ORDER BY custdist DESC, c_count DESC;
