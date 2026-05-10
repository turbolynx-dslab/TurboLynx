MATCH 
(ord:ORDERS)-[:MADE_BY]->(c:CUSTOMER),
(lineitem:LINEITEM)-[:IS_PART_OF]->(ord),
(lineitem)-[:SUPPLIED_BY]->(s:SUPPLIER),
(s)-[:SUPP_BELONG_TO]->(nation:NATION),
(c)-[:CUST_BELONG_TO]->(nation),
(nation)-[:IS_LOCATED_IN]->(region:REGION)
WHERE 
region.R_NAME = "EUROPE" AND 
date(ord.O_ORDERDATE) >= date("1994-01-01") 
AND date(ord.O_ORDERDATE) < date("1995-01-01")
RETURN nation.N_NAME, SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS REVENUE
ORDER BY REVENUE DESC;