MATCH (li:LINEITEM)-[:COMPOSED_BY]->(p:PART)
MATCH (li)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n2:NATION)
MATCH (li)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n1:NATION)
MATCH (n1)-[:IS_LOCATED_IN]->(r:REGION)
WHERE r.R_NAME = "AFRICA"
AND date(o.O_ORDERDATE) > date("1995-01-01")
AND date(o.O_ORDERDATE) < date("1996-12-31")
AND p.P_TYPE = "ECONOMY BRUSHED BRASS"
WITH o.O_ORDERDATE.year as o_year, sum(li.L_EXTENDEDPRICE * (1-tofloat(li.L_DISCOUNT))) as volume, n2.N_NAME as nation
RETURN distinct o_year, 
sum(CASE WHEN nation = "ETHIOPIA"
    THEN volume
    ELSE 0 END) / sum(volume) as mkt_share
order by o_year;