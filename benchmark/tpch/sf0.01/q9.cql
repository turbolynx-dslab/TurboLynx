MATCH 
(li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER),
(li)-[:COMPOSED_BY]->(p:PART),
(li)-[:IS_PART_OF]->(o:ORDERS),
(p)-[ps:PARTSUPP]->(s),
(s)-[:SUPP_BELONG_TO]->(n:NATION)
WHERE p.P_NAME contains "salmon"
WITH li,ps,n,o 
ORDER BY n.N_NAME, o.O_ORDERDATE.year desc
RETURN n.N_NAME as nation, o.O_ORDERDATE.year as year,
sum(li.L_EXTENDEDPRICE * (1 -li.L_DISCOUNT)-ps.PS_SUPPLYCOST * li.L_QUANTITY) as amount;