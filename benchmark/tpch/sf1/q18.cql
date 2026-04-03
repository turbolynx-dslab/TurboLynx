MATCH (lineitem: LINEITEM)
WITH lineitem.L_ORDERKEY as l_orderkey, SUM(lineitem.L_QUANTITY) AS sum_lquantity
WHERE sum_lquantity > 315
MATCH (item: LINEITEM)-[:IS_PART_OF]->(o: ORDERS)-[:MADE_BY]->(customer: CUSTOMER)
WHERE o.O_ORDERKEY IN [l_orderkey]
RETURN customer.C_NAME, customer.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(item.L_QUANTITY)
ORDER BY o.O_TOTALPRICE desc, o.O_ORDERDATE
LIMIT 100;