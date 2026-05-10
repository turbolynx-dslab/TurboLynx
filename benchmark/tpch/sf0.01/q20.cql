MATCH
(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n:NATION),
(p:PART)-[ps:PARTSUPP]->(s),
(li:LINEITEM)-[:COMPOSED_BY]->(p),
(li)-[:SUPPLIED_BY]->(s)
WHERE
n.N_NAME = "BRAZIL" and
p.P_NAME STARTS WITH "smoke" and
li.L_SHIPDATE >= date("1997-01-01") and
li.L_SHIPDATE < date("1998-01-01")
WITH ps, s, 0.5 * sum(li.L_QUANTITY) as quantity_sum
WHERE ps.PS_AVAILQTY > quantity_sum
RETURN distinct s.S_NAME, s.S_ADDRESS
ORDER BY s.S_NAME;