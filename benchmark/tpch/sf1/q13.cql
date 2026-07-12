MATCH (customer: CUSTOMER)
OPTIONAL MATCH (customer)<-[:MADE_BY]-(ord: ORDERS)
WHERE NOT (ord.O_COMMENT =~ ".*express.*deposits.*")
WITH customer, CASE WHEN ord IS NOT NULL THEN ord.O_ORDERKEY ELSE null END AS ok
WITH customer.C_CUSTKEY AS c_id, COUNT(ok) AS c_count
RETURN c_count, COUNT(*) AS custdist
ORDER BY custdist DESC, c_count DESC;
