MATCH (li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n1:NATION)
MATCH (li)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n2:NATION) 
WHERE 
((n1.N_NAME = "IRAN" and n2.N_NAME = "ETHIOPIA")
OR (n1.N_NAME = "ETHIOPIA" and n2.N_NAME = "IRAN"))
AND li.L_SHIPDATE >= date("1995-01-01")
AND li.L_SHIPDATE <= date("1996-12-31")
RETURN n1.N_NAME as supp_nation, n2.N_NAME as cust_nation, li.L_SHIPDATE.year as l_year, sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as volume
order by supp_nation, cust_nation, l_year;