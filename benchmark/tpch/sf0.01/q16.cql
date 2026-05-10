MATCH (p:PART)-[ps:PARTSUPP]->(s:SUPPLIER)
WHERE p.P_BRAND <> "Brand#51" 
and NOT (p.P_TYPE STARTS WITH "PROMO PLATED")
and p.P_SIZE in [11, 44, 42, 8, 45, 14, 40, 46]
and NOT s.S_COMMENT =~ ".*Customer.*Complaints.*" 
RETURN p.P_BRAND,p.P_TYPE,p.P_SIZE,count(distinct s.S_SUPPKEY) as supplier_cnt
ORDER BY supplier_cnt desc, p.P_BRAND, p.P_TYPE, p.P_SIZE;