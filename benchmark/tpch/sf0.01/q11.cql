MATCH (pa:PART)-[p:PARTSUPP]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n:NATION)
WHERE n.N_NAME = "ROMANIA"
WITH sum(p.PS_SUPPLYCOST * p.PS_AVAILQTY) * 0.0001 as subquery
MATCH (pa2:PART)-[p2:PARTSUPP]->(s2:SUPPLIER)-[:SUPP_BELONG_TO]->(n2:NATION)
WHERE n2.N_NAME = "ROMANIA"
WITH pa2.P_PARTKEY as partkey, sum(p2.PS_SUPPLYCOST * p2.PS_AVAILQTY) as value, subquery
WHERE value > subquery
RETURN partkey, value
ORDER BY value desc;